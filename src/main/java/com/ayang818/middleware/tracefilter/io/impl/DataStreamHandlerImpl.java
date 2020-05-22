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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(3);

    private static WebSocket wsclient = null;

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

    private static Map<String, List<String>> cacheErrTraceMap = new ConcurrentHashMap<>(1000);

    private static AtomicInteger freeTime = new AtomicInteger(0);

    public static WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    logger.info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    THREAD_POOL.execute(() -> {
                        JSONObject res = JSON.parseObject(payload);
                        String resType = (String) res.get("resType");

                        if (Objects.equals(resType, "free")) {
                            Set<String> recSet = res.getObject("data", new TypeReference<Set<String>>() {
                            });
                            Integer minLineNumber = res.getObject("minLineNumber", Integer.class);
                            freeTime.getAndAdd(1);
                            logger.info("第 {} 次标记和释放链路...", freeTime.get());

                            // 增加新的traceId
                            errTraceIdSet.addAll(recSet);
                            Iterator<Map.Entry<String, Trace>> it = TRACE_MAP.entrySet().iterator();
                            Map.Entry<String, Trace> entry;
                            // 这里又对整个trace_map进行了一次扫描
                            while (it.hasNext()) {
                                entry = it.next();
                                // 没有上报的错误链路，标记
                                if (!errTraceIdSet.contains(entry.getKey())) {
                                    entry.getValue().setAsErrorTrace();
                                } else {
                                    if (minLineNumber - entry.getValue().getFirstOccurrenceLine() >= 40000) {
                                        it.remove();
                                    }
                                }
                            }
                            logger.info("结束释放和标记链路...");
                        } else if (Objects.equals(resType, "last")) {
                            Set<String> recSet = res.getObject("data", new TypeReference<Set<String>>() {
                            });
                            errTraceIdSet.addAll(recSet);
                            Iterator<Map.Entry<String, Trace>> it = TRACE_MAP.entrySet().iterator();
                            Map.Entry<String, Trace> entry;
                            // 这里又对整个trace_map进行了一次扫描
                            while (it.hasNext()) {
                                entry = it.next();
                                // 没有上报的错误链路，标记
                                if (errTraceIdSet.contains(entry.getKey())) {
                                    entry.getValue().setAsErrorTrace();
                                }
                            }

                            sendTraceMsg(false);

                            // 发送最终的realFin
                            String msg = String.format("{\"type\": %d}", REAL_FIN_TYPE);
                            wsclient.sendTextFrame(msg);
                        } else {
                            logger.warn("接收到异常数据! {}", payload);
                        }

                    });
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
        long startTime = System.currentTimeMillis();

        // 在第几行检查Map
        int checkLine = 40000;

        wsclient = WsClient.getWebSocketClient();

        BufferedReader bf = new BufferedReader(new InputStreamReader(dataStream), 1024 * 16);
        String lineData;

        try {
            while ((lineData = bf.readLine()) != null) {
                lineNumber.getAndAdd(1);

                // 检查当前行号是否到达需要检查的分界线，若到达，检查Map，并将checkLine后移
                if (lineNumber.get() > checkLine) {
                    scanNum.getAndAdd(1);
                    logger.info("开始进行第 {} 次上报数据", scanNum.get());

                    sendTraceMsg(false);

                    checkLine += SEND_TRACE_MAP_INTERVAL;
                    logger.info("上报结束");
                }

                handleLine(lineData, lineNumber.get());

            }

            sendTraceMsg(true);

            logger.info("拉取数据源完毕，耗时 {} ms......", System.currentTimeMillis() - startTime);
            logger.info("共检测到 {} 条Trace出错", errTraceIdSet.size());
            logger.info("共拉到 {} 行数据", lineNumber);
        } catch (IOException e) {
            logger.warn("读取数据流出错");
        }
    }

    private static void sendTraceMsg(boolean isFin) {
        // send lineNumber, cacheErrTraceMap, type
        // {lineNumber: %d, type: %d, data: %s} data: {traceId: List<String>}
        for (String traceId : errTraceIdSet) {
            Trace trace = TRACE_MAP.get(traceId);
            if (trace != null && trace.getSpans() != null && lineNumber.get() - trace.getFirstOccurrenceLine() > SEND_THRESHOLD) {
                cacheErrTraceMap.put(traceId, trace.getSpans());
                TRACE_MAP.remove(traceId);
            }
        }

        String msg = String.format("{\"type\": %d, \"lineNumber\": %d, \"data\": %s}", ERROR_TYPE, lineNumber.get(), JSON.toJSONString(cacheErrTraceMap));
        wsclient.sendTextFrame(msg);
        cacheErrTraceMap.clear();

        if (isFin) {
            logger.info("已准备发送终止请求......");
            if (wsclient.isOpen()) {
                String tmpMsg = String.format("{\"type\": %d}", FIN_TYPE);
                wsclient.sendTextFrame(tmpMsg);
                logger.info("终止请求发送成功......");
            } else {
                logger.info("连接已断开，终止请求发送失败......");
            }
        }

        // 发送拉取所有错误链路的请求
        wsclient.sendTextFrame(PULL_ERR_MSG);
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
        updateDataAfterAnalyzeLine(traceId, lineData, lineNumber, startTime);

        if (isWrong) {
            handleWrongLine(traceId, lineData);
            return true;
        }
        return false;
    }

    /**
     * @param traceId  此行的traceId
     * @param lineData 此行的内容
     * @description 标记错误链路
     */
    private void handleWrongLine(String traceId, String lineData) {
        // 所有错误Id
        errTraceIdSet.add(traceId);

        // 对于错误行，先将map中对应的traceId对应的trace设置为问题trace
        TRACE_MAP.computeIfPresent(traceId, (id, trace) -> {
            trace.setAsErrorTrace();
            return trace;
        });
    }

    /**
     * @param traceId
     * @param lineData
     * @param lineNumber
     * @param startTime
     * @description 更新map
     */
    private void updateDataAfterAnalyzeLine(String traceId, String lineData, Integer lineNumber, String startTime) {
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
}