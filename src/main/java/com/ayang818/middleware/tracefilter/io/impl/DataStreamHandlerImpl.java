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

    private static final ConcurrentHashMap<String, Trace> TRACE_MAP =
            new ConcurrentHashMap<>(8192);

    private static final HashMap<String, Integer> tmpMap = new HashMap<>();

    private static final ExecutorService SINGLE_TRHEAD = Executors.newSingleThreadExecutor();

    private static WebSocket wsclient = null;

    private static AtomicInteger totalErrSum = new AtomicInteger(0);

    private static AtomicInteger scanNum = new AtomicInteger(0);

    private static Set<String> errTraceIdSet = Sets.newConcurrentHashSet();

    private static final List<String> PENDING_FRAME_BUFFER = new ArrayList<>(PENDING_BUFFER_SIZE);

    public static final WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    logger.info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    // 在回调中接收 形如 {traceId: xxx, resType: "fin/error"}
                    // if resType == free, delete the corresponding entry locally and free memory
                    // if resType == error, report the corresponding entry to the data merge central
                    JSONObject res = JSON.parseObject(payload);
                    String resType = (String) res.get("resType");
                    logger.info("收到消息类型为 {}", resType);
                    if (Objects.equals(resType, "free")) {
                        // TODO free these true traces
                        logger.info("开始释放和标记链路......");
                        // 错误链路
                        Set<String> recSet = res.getObject("data", new TypeReference<Set<String>>(){});
                        SINGLE_TRHEAD.execute(() -> {
                            Set<Map.Entry<String, Trace>> entries = TRACE_MAP.entrySet();
                            for (Map.Entry<String, Trace> entry : entries) {
                                if (recSet.contains(entry.getKey())) {
                                    entry.getValue().setAsErrorTrace();
                                    errTraceIdSet.add(entry.getKey());
                                } else {
                                    //TRACE_MAP.remove(entry.getKey());
                                }
                            }
                        });
                        logger.info("结束释放和标记链路......");
                    } else {
                        logger.warn("接收到异常数据! {}", payload);
                    }
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
        wsclient = WsClient.getWebSocketClient(wsHandler);
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

        // 行号
        int lineNumber = 0;

        // 错误行数
        int wrongLineNumber = 0;

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
            if (lineNumber > checkLine) {
                scanMap(lineNumber, false);
                //scanMapParallely(lineNumber, false);

                checkLine += SCAN_TRACE_MAP_INTERVAL;
            }

            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == '\n') {
                    // 行号
                    lineNumber += 1;
                    // 得到一行数据
                    String lineData = strbuilder.toString();
                    // 统计收到的字节数，这个没啥用
                    sumbytes += lineData.getBytes().length;
                    // 处理一行数据；返回是否为错误行；
                    wrongLineNumber += handleLine(lineData, lineNumber) ? 1 : 0;
                    strbuilder.delete(0, strbuilder.length());
                } else {
                    strbuilder.append(chars[i]);
                }
            }
        }

        // 在所有数据读取结束后，进行最后一次检查
        //scanMapParallely(lineNumber, true);
        scanMap(lineNumber, true);

        logger.info("共有 {} 行数据出错", wrongLineNumber);
        logger.info("共有 {} 条Trace出错", errTraceIdSet.size());
        logger.info("共上报 {} 条错误Trace", totalErrSum.get());
        logger.info("拉取数据源完毕，耗时 {} ms......", System.currentTimeMillis() - startTime);
        logger.info("共拉到 {} 行数据，每行数据平均大小 {} 字节", lineNumber, sumbytes / lineNumber);

        int minTraceSpanNum = Integer.MAX_VALUE;
        int maxTraceSpanNum = Integer.MIN_VALUE;
        long spanSum = 0;
        for (Map.Entry<String, Integer> entry : tmpMap.entrySet()) {
            minTraceSpanNum = Math.min(entry.getValue(), minTraceSpanNum);
            maxTraceSpanNum = Math.max(entry.getValue(), maxTraceSpanNum);
            spanSum += entry.getValue();
        }
        logger.info("共有 {} 条trace", tmpMap.size());
        logger.info("trace中span最多为 {} 条，span最少为 {} 条，平均为 {} 条", maxTraceSpanNum, minTraceSpanNum, spanSum / tmpMap.size());

    }

    @Override
    public boolean handleLine(String lineData, Integer lineNumber) {
        if (lineData == null) {
            logger.info("此行为空, 行号 {}", lineNumber);
            return false;
        }
        // 每行数据总共9列，以8个 | 号做分割
        String[] data = SplitterUtil.baseSplit(lineData, "\\|");

        // 去除掉可能导致程序报错的数据，这一部分数据过滤掉应该不会对我的代码造成太大的影响
        if (data.length < 9) {
            logger.info("不符合格式的数据 : {}, 行号 {}", lineData, lineNumber);
            return false;
        }

        String traceId = data[0];
        String startTime = data[1];
        String tags = data[8];

        // 统计trace数量(only for test)
        tmpMap.computeIfPresent(traceId, (key, value) -> value + 1);
        tmpMap.putIfAbsent(traceId, 0);

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
     * @description 对于错误行的处理
     */
    private void handleWrongLine(String traceId, String lineData, Integer lineNumber) {
        // 用于记录错误 trace 数量
        this.errTraceIdSet.add(traceId);
        // 对于错误行，先将map中对应的traceId对应的trace设置为问题trace
        TRACE_MAP.compute(traceId, (id, trace) -> {
            trace.setAsErrorTrace();
            return trace;
        });
    }

    private void updateMapAfterAnalyzeLine(String traceId, String lineData, Integer lineNumber, String startTime) {
        // 更新数据
        if (TRACE_MAP.containsKey(traceId)) {
            TRACE_MAP.computeIfPresent(traceId, (id, trace) -> {
                trace.addNewSpan(lineData, lineNumber, startTime);
                return trace;
            });
        } else {
            Trace trace = new Trace(traceId);
            trace.addNewSpan(lineData, lineNumber, startTime);
            TRACE_MAP.put(traceId, trace);
        }
    }

    /**
     * @param lineNumber 在开始这次扫描时，解析到的行号，在扫描的过程中，这个数字不会改变
     * @param isFin      是否是最后一次扫描
     */
    private void scanMap(Integer lineNumber, Boolean isFin) {
        long startTime = System.currentTimeMillis();
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

            // 10w 作为一个threshold；对于那些本地没有问题的trace，
            // 而其他过滤容器又没有这条traceId的信息，删除。
            //if (lineNumber - trace.getLastOccurrenceLine() >= REMOVE_ANYWAY_THRESHOLD) {
            //    TRACE_MAP.remove(trace.getTraceId());
            //}

            // 如果当前行和这条链路出现的最后一条链路行号相差 2w 以上，上报数据后端检查，
            if (isFin || lineNumber - trace.getLastOccurrenceLine() >= SCAN_TRACE_MAP_INTERVAL) {

                if (trace.isNormalTrace()) {
                    //if (PENDING_FRAME_BUFFER.size() <= PENDING_BUFFER_SIZE) {
                    //    PENDING_FRAME_BUFFER.add(String.format("{\"traceId\": \"%s\", \"curLine\": %d}", trace.getTraceId(), trace.getLastOccurrenceLine()));
                    //} else {
                    //    // 发送消息，清空 pending 数据缓冲区
                    //    String msg = String.format("{\"type\": %d,\"data\": %s}", PENDING_TYPE, PENDING_FRAME_BUFFER.toString());
                    //    wsclient.sendTextFrame(msg);
                    //    PENDING_FRAME_BUFFER.clear();
                    //
                    //    sendTrueTimes += 1;
                    //    sendFrameTimes += 1;
                    //}
                    //// 对于在本条数据流中无误的链路，向数据汇总容器上报一次pending请求

                    // TODO 如果是正常链路咋办呢？不上报？
                } else {
                    // 如果是一条有问题的链路，那么就直接发把当前本地所有数据上报即可；
                    // 由于当前行数已经相差2w，所以默认在本机是不会产生新的数据的。
                    trace.getSpans().sort((span1, span2) -> span1.getStartTime() - span2.getStartTime() > 0 ? 1 : 0);
                    String msg = String.format("{\"traceId\": \"%s\", \"curLine\": %d, \"spans\": %s, \"type\": %d}", trace.getTraceId(), trace.getLastOccurrenceLine(), trace.getSpans().toString(), ERROR_TYPE);
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
            String msg = String.format("{\"type\": %d}", FIN_TYPE);
            wsclient.sendTextFrame(msg);
        }

        // 发送拉取所有错误链路的请求
        wsclient.sendTextFrame(PULL_ERR_MSG);


        scanNum.addAndGet(1);

        logger.info("第 {} 次扫描结束Trace_Map, 共扫描 {} 个对象, 共发送 {} 次frame，其中 {} 次为错误，{} 次为正确，共耗时 {} 毫秒。", scanNum.get(), size, sendFrameTimes, sendErrorTimes, sendTrueTimes, System.currentTimeMillis() - startTime);
    }

    //private void scanMapParallely(Integer lineNumber, Boolean isFin) {
    //    SINGLE_TRHEAD.execute(() -> {
    //        scanMap(lineNumber, isFin);
    //    });
    //}

}