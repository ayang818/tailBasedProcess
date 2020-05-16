package com.ayang818.middleware.tracefilter.io.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.pojo.dto.Trace;
import com.ayang818.middleware.tracefilter.utils.SplitterUtil;
import com.ayang818.middleware.tracefilter.utils.WsClient;
import io.netty.channel.ChannelFuture;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:47
 **/
@Service
public class DataStreamHandlerImpl implements DataStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(DataStreamHandlerImpl.class);

    /**
     * 默认读缓冲区大小，由于系统默认的用户地址空间文件缓冲区大小4096字节，所以这里取4096的倍数4MB
     */
    private final Integer defaultReadBufferSize = 1024 * 1024 * 4;

    /**
     * 换行标识符
     */
    private static final char NEXT_LINE_FLAG = '\n';

    /**
     * 大部分单条数据长度（byte）
     */
    private static final Integer NORMAL_PER_LINE_SIZE = 270;

    private static final String STATUS_CODE = "http.status_code";

    private static final String PASS_STATUS_CODE = "200";

    private static final String ERROR = "error";

    private static final String BAN_ERROR_CODE = "1";

    private static final ConcurrentHashMap<String, Trace> TRACE_MAP =
            new ConcurrentHashMap<>(2048);

    private static final HashMap<String, Integer> tmpMap = new HashMap<>();

    private static final ExecutorService SINGLE_TRHEAD = Executors.newSingleThreadExecutor();

    private static WebSocket wsclient = null;

    private static final Integer REMOVE_ANYWAY_THRESHOLD = 100000;

    private static final Integer CHECK_TRACE_MAP_INTERVAL = 20000;

    public static final WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    logger.info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    // 在回调中接收 形如 {traceId: xxx, respType: "fin/error"}
                    // if resType == fin, delete the corresponding entry locally and free memory
                    // if resType == error, report the corresponding entry to the data merge central
                    JSONObject res = JSON.parseObject(payload);
                    String resType = (String) res.get("resType");

                    if (Objects.equals(resType, "fin")) {
                        String traceId = (String) res.get("traceId");
                        TRACE_MAP.remove(traceId);
                    } else if (Objects.equals(resType, "error")) {
                        String traceId = (String) res.get("traceId");
                        Trace trace = TRACE_MAP.get(traceId);
                        if (trace != null) {
                            wsclient.sendTextFrame(JSON.toJSONString(trace));
                        } else {
                            logger.warn("诡异的错误，TraceMap中不存在该traceId");
                        }
                    } else {
                        logger.warn("异常数据! {}", payload);
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
            return ;
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
        int checkLine = 30000;

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
                checkMap(lineNumber);
                checkMapParallel(lineNumber);

                checkLine += CHECK_TRACE_MAP_INTERVAL;
            }

            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == NEXT_LINE_FLAG) {
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

        logger.info("共有 {} 行数据出错", wrongLineNumber);
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
        String tags = data[8];

        // 统计trace数量(only for test)
        tmpMap.computeIfPresent(traceId, (key, value) -> value+1);
        tmpMap.putIfAbsent(traceId, 0);

        // 是否为错误行
        boolean isWrong = false;

        // 遍历所有 tag，查看这条span是否符合过滤要求
        String[] eachTag = SplitterUtil.baseSplit(tags, "&");

        for (int i = 0; i < eachTag.length; i++) {
            String[] kvArray = SplitterUtil.baseSplit(eachTag[i], "=");
            if (kvArray != null && kvArray.length == 2) {
                String key = kvArray[0];
                String val = kvArray[1];
                boolean satisfyStatusCode =
                        STATUS_CODE.equals(key) && !PASS_STATUS_CODE.equals(val);
                boolean satisfyError = ERROR.equals(key) && BAN_ERROR_CODE.equals(val);
                // 过滤条件 : http.status_code!=200 || error=1
                if (satisfyStatusCode || satisfyError) {
                    isWrong = true;
                    break;
                }
            }
        }

        // 更新map
        updateMapAfterAnalyzeLine(traceId, lineData, lineNumber);

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
        // 对于错误行，先将map中对应的traceId对应的trace设置为问题trace
        TRACE_MAP.compute(traceId, (id, trace) -> {
            trace.setAsErrorTrace();
            return null;
        });
    }

    private void updateMapAfterAnalyzeLine(String traceId, String lineData, Integer lineNumber) {
        // 更新数据
        if (TRACE_MAP.containsKey(traceId)) {
            TRACE_MAP.compute(traceId, (id, trace) -> {
                trace.addNewSpan(lineData, lineNumber);
                return trace;
            });
        } else {
            Trace trace = new Trace(traceId);
            trace.addNewSpan(lineData, lineNumber);
            TRACE_MAP.put(traceId, trace);
        }
    }

    /**
     * @description 检查map
     * @param lineNumber 当前行号
     */
    private void checkMap(Integer lineNumber) {
        Iterator<Map.Entry<String, Trace>> it = TRACE_MAP.entrySet().iterator();
        Map.Entry<String, Trace> entry;

        Integer sendFrameTimes = 0;
        Integer sendErrorTime = 0;
        Integer sendTrueTime = 0;
        boolean flag = true;

        while (it.hasNext()) {
            entry = it.next();
            Trace trace = entry.getValue();

            // 10w 作为一个threshold；对于那些本地没有问题的trace，
            // 而其他过滤容器又没有这条traceId的信息，删除。
            if (lineNumber - trace.getLastOccurrenceLine() > REMOVE_ANYWAY_THRESHOLD) {
                TRACE_MAP.remove(trace.getTraceId());
            }

            // 如果当前行和这条链路出现的最后一条链路行号相差 2w 以上，上报数据中心检查，
            if (lineNumber - trace.getLastOccurrenceLine() > CHECK_TRACE_MAP_INTERVAL) {

                sendFrameTimes += 1;
                // report to data central

                if (!trace.isNormalTrace()) {
                    // 如果是一条有问题的链路，那么就直接发把当前本地所有数据上报即可；
                    // 由于当前行数已经相差2w，所以默认在本机是不会产生新的数据的。
                    wsclient.sendTextFrame(JSON.toJSONString(trace));
                    // 删除Map中的键值，释放内存
                    TRACE_MAP.remove(trace.getTraceId());
                    sendErrorTime += 1;
                } else {
                    sendTrueTime += 1;
                    // 对于在本条数据流中无误的链路，向数据汇总容器上报一次pending请求
                    wsclient.sendTextFrame("{\n" +
                            "  \"traceId\": "+ trace.getTraceId() +",\n" +
                            "  \"type\": \"pending\",\n" +
                            "  \"curLine\": "+ lineNumber + "\n" +
                            "}");
                }
            }
        }

        if (flag) {
            logger.info("一次map检查中需要发送 {} 次 WebSocketTextFrame。其中 {} 次错误消息， {} 次正确消息", sendFrameTimes, sendErrorTime, sendTrueTime);
            flag = false;
        }

    }

    private void checkMapParallel(Integer lineNumber) {
        SINGLE_TRHEAD.execute(() -> {

        });
    }

}