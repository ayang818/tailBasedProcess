package com.ayang818.middleware.tracefilter.io.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.pojo.dto.Trace;
import com.ayang818.middleware.tracefilter.utils.SplitterUtil;
import com.ayang818.middleware.tracefilter.utils.WsClient;
import com.google.common.collect.Sets;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ayang818.middleware.tracefilter.consts.Setting.*;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:47
 **/
@Service
public class DataStreamHandlerImpl implements DataStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(DataStreamHandlerImpl.class);

    /**
     * traceId 到 Trace 具体内容的映射
     */
    private static final ConcurrentHashMap<String, Trace> TRACE_MAP =
            new ConcurrentHashMap<>(8192);

    private static final ExecutorService SINGLE_TRHEAD = Executors.newSingleThreadExecutor();

    private static WebSocket wsclient = null;

    /**
     * 本容器中总共上报的错误trace数量
     */
    private static AtomicInteger totalErrSum = new AtomicInteger(0);

    /**
     * 扫描次数
     */
    private static AtomicInteger scanNum = new AtomicInteger(0);

    /**
     * 错误链路的集合
     */
    private static Set<String> errTraceIdSet = Sets.newConcurrentHashSet();

    /**
     * 行号
     */
    private static AtomicInteger lineNumber = new AtomicInteger(0);

    public static final WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    logger.info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    logger.info("接收到数据中的回复，开始处理消息......");
                    JSONObject res = JSON.parseObject(payload);
                    String resType = (String) res.get("resType");
                    if (lineNumber.get() < 100000) {
                        return ;
                    }
                    if (Objects.equals(resType, "free")) {
                        logger.info("开始标记和释放链路......");
                        Set<String> recSet = res.getObject("data", new TypeReference<Set<String>>() {
                        });
                        SINGLE_TRHEAD.execute(() -> {
                            // 增加新的traceId
                            errTraceIdSet.addAll(recSet);
                            Set<Map.Entry<String, Trace>> entries = TRACE_MAP.entrySet();
                            Iterator<Map.Entry<String, Trace>> it = entries.iterator();
                            Map.Entry<String, Trace> entry;
                            // 这里又对整个trace_map进行了一次扫描
                            while (it.hasNext()) {
                                entry = it.next();
                                if (errTraceIdSet.contains(entry.getKey())) {
                                    entry.getValue().setAsErrorTrace();
                                } else {
                                    //// 这里容易删除掉不该删除的数据
                                    //if (entry.getKey().equals("6a23ffc5521fc3eb")) {
                                    //    logger.info("删除了不该删除的数据!!!!!!!!!!!!!!!!!!!");
                                    //}
                                    if (lineNumber.get() - entry.getValue().getFirstOccurrenceLine() >= 100000) {
                                        it.remove();
                                    }
                                }
                            }
                        });
                        logger.info("结束释放和标记链路......");
                    } else {
                        logger.warn("接收到异常数据! {}", payload);
                    }
                    logger.info("处理后端消息结束");
                }

                @Override
                public void onClose(WebSocket websocket, int code, String reason) {
                    // WebSocket connection closed
                    logger.info("websocket 连接已断开......");
                }

                @Override
                public void onError(Throwable t) {
                    // WebSocket connection error
                    logger.error("websocket 连接发生错误，堆栈信息如下......");
                    t.printStackTrace();
                }

            }).build();

    @Override
    public void handleDataStream(InputStream dataStream) {
        logger.info("连接数据源成功，开始拉取数据......");
        wsclient = WsClient.getWebSocketClient();
        if (wsclient == null) {
            logger.info("连接建立失败......");
            return;
        }
        logger.info("同数据汇总中心长连接建立成功......");
        ReadableByteChannel inChannel = Channels.newChannel(dataStream);
        // set nio read Buffer
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(defaultReadBufferSize);
        try {
            filterLine(inChannel, readByteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void filterLine(ReadableByteChannel inChannel, ByteBuffer readByteBuffer) throws IOException {
        // 起始时间
        long startTime = System.currentTimeMillis();

        // 读暂存数组
        byte[] bytes = new byte[defaultReadBufferSize];

        // 行字符串构造器
        StringBuilder strbuilder = new StringBuilder();

        // 总字节数，用于统计数据是否有坑 + 为后续多线程拉取速度对比提供分段标准
        long sumbytes = 0;

        // 在第几行检查Map
        int checkLine = 40000;

        while (inChannel.read(readByteBuffer) != -1) {
            // 反转准备读
            readByteBuffer.flip();
            int bytesLen = readByteBuffer.remaining();
            // 读入临时bytes数组
            readByteBuffer.get(bytes, 0, bytesLen);
            readByteBuffer.clear();
            String tmpstr = new String(bytes, 0, bytesLen);
            char[] chars = tmpstr.toCharArray();

            // 检查当前行号是否到达需要检查的分界线，若到达，检查Map，并将checkLine后移
            if (lineNumber.get() > checkLine) {
                scanMap(lineNumber.get(), false);
                //scanMapParallely(lineNumber, false);

                checkLine += SCAN_TRACE_MAP_INTERVAL;
            }

            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == '\n') {
                    // 行号
                    lineNumber.addAndGet(1);
                    // 得到一行数据
                    String lineData = strbuilder.toString();
                    // 统计收到的字节数，这个没啥用
                    sumbytes += lineData.getBytes().length;
                    // 处理一行数据；返回是否为错误行；
                        handleLine(lineData, lineNumber.get());
                    strbuilder.delete(0, strbuilder.length());
                } else {
                    strbuilder.append(chars[i]);
                }
            }
        }

        // 在所有数据读取结束后，进行最后一次检查
        //scanMapParallely(lineNumber, true);
        scanMap(lineNumber.get(), true);

        logger.info("拉取数据源完毕，耗时 {} ms......", System.currentTimeMillis() - startTime);
        logger.info("共检测到 {} 条Trace出错", errTraceIdSet.size());
        logger.info("共上报 {} 条错误Trace", totalErrSum.get());
        logger.info("共拉到 {} 行数据，每行数据平均大小 {} 字节", lineNumber, sumbytes / lineNumber.get());

    }

    @Override
    public boolean handleLine(String lineData, Integer lineNumber) {
        if (lineData == null) {
            logger.info("此行为空, 行号 {}", lineNumber);
            return false;
        }
        // 每行数据总共9列，以8个 | 号做分割
        String[] data = SplitterUtil.baseSplit(lineData, "\\|");

        // 去除掉会导致程序报错的数据，这一部分数据过滤掉应该不会对我的代码造成太大的影响
        if (data.length < 9) {
            return false;
        }

        String traceId = data[0];
        String startTime = data[1];
        String tags = data[8];

        // 是否为错误行
        boolean isWrong = false;

        if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
            isWrong = true;
        }

        // 更新map
        updateMapAfterAnalyzeLine(traceId, lineData, lineNumber, startTime);

        if (isWrong) {
            handleWrongLine(traceId, lineData, lineNumber);
            return true;
        }
        return false;
    }

    /**
     * @param traceId    此行的traceId
     * @param lineData   此行的内容
     * @param lineNumber 此行的行号
     * @description      标记错误链路
     */
    private void handleWrongLine(String traceId, String lineData, Integer lineNumber) {
        // 用于记录错误 trace 数量
        errTraceIdSet.add(traceId);
        // 对于错误行，先将map中对应的traceId对应的trace设置为问题trace
        TRACE_MAP.computeIfPresent(traceId, (id, trace) -> {
            trace.setAsErrorTrace();
            return trace;
        });
    }

    /**
     * @description 更新map
     * @param traceId
     * @param lineData
     * @param lineNumber
     * @param startTime
     */
    private void updateMapAfterAnalyzeLine(String traceId, String lineData, Integer lineNumber, String startTime) {
        // 更新数据
        if (TRACE_MAP.containsKey(traceId)) {
            TRACE_MAP.computeIfPresent(traceId, (id, trace) -> {
                trace.addNewSpan(lineData, startTime);
                return trace;
            });
        } else {
            Trace trace = new Trace(traceId, lineNumber);
            trace.addNewSpan(lineData, startTime);
            TRACE_MAP.put(traceId, trace);
        }
    }

    /**
     * @param lineNumber 在开始这次扫描时，解析到的行号，在扫描的过程中，这个数字不会改变
     * @param isFin      是否是最后一次扫描
     */
    private void scanMap(Integer lineNumber, Boolean isFin) {
        int size = TRACE_MAP.size();
        Iterator<Map.Entry<String, Trace>> it = TRACE_MAP.entrySet().iterator();
        Map.Entry<String, Trace> entry;

        int sendFrameTimes = 0;
        int sendErrorTimes = 0;
        int sendTrueTimes = 0;

        Trace trace;

        while (it.hasNext()) {
            entry = it.next();
            trace = entry.getValue();

            // 如果当前行和这条链路出现的最早出现的一条数据行号相差 2w 以上，上报数据后端检查，
            if (isFin || lineNumber - trace.getFirstOccurrenceLine() >= SCAN_TRACE_MAP_INTERVAL) {

                if (!trace.isNormalTrace()) {
                    // 如果是一条有问题的链路，那么就直接发把当前本地所有数据上报即可；
                    // 由于当前行数已经相差2w，所以默认在本机是不会产生新的数据的。
                    trace.getSpans().sort((span1, span2) -> span1.getStartTime() - span2.getStartTime() > 0 ? 1 : 0);

                    String msg = String.format("{\"traceId\": \"%s\", \"curLine\": %d, \"spans\": %s, \"type\": %d}", trace.getTraceId(), trace.getFirstOccurrenceLine(), trace.getSpans().toString(), ERROR_TYPE);
                    wsclient.sendTextFrame(msg);

                    // 删除Map中的键值，释放内存
                    TRACE_MAP.remove(trace.getTraceId());

                    sendErrorTimes += 1;
                    sendFrameTimes += 1;

                    // 用于确保在本条链路一定错误的上报率为 100%
                    totalErrSum.getAndAdd(1);
                }

            }
        }

        // 最后一次扫描完后，向数据后端发送 FIN 消息，告知数据已经过滤发送完毕，可以进行下一步处理了
        if (isFin) {
            logger.info("已准备发送终止请求......");
            if (wsclient.isOpen()) {
                String msg = String.format("{\"type\": %d}", FIN_TYPE);
                wsclient.sendTextFrame(msg);
                logger.info("终止请求发送成功......");
            } else {
                logger.info("连接已断开，终止请求发送失败......");
            }
        }

        // 发送拉取所有错误链路的请求
        wsclient.sendTextFrame(PULL_ERR_MSG);

        scanNum.addAndGet(1);

        logger.info("第 {} 次扫描结束Trace_Map, 共扫描 {} 个对象, 共发送 {} 次frame，其中 {} 次为错误，{} 次为正确。", scanNum.get(), size, sendFrameTimes, sendErrorTimes, sendTrueTimes);
    }

}