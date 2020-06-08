package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.CommonController;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.ayang818.middleware.tailbase.client.DataStorage.*;

/**
 * @author 杨丰畅
 * @description client 端用于处理读入数据流，向 backend 发送对应请求的类
 * @date 2020/5/5 12:47
 **/
@Service
public class ClientDataStreamHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ClientDataStreamHandler.class);
    private static WebSocket sendWebSocket;
    // 行号
    private static long lineCount = 0;
    // 第 pos 轮， 不取余
    private static int pos = 0;
    // bucketPos 在bucketList中的位置，pos % BUCKET_COUNT
    private static int bucketPos = 0;

    public static void init() {
        for (int i = 0; i < Constants.CLIENT_BUCKET_COUNT; i++) {
            BUCKET_TRACE_LIST.add(new TraceCacheBucket(Constants.CLIENT_BUCKET_MAP_SIZE));
            ERR_TRACE_SET_LIST.add(new HashSet<>(Constants.BUCKET_ERR_TRACE_COUNT));
        }
    }

    public static void start() {
        START_POOL.execute(new ClientDataStreamHandler());
    }

    @Override
    public void run() {
        try {
            sendWebSocket = WsClient.getSendWebsocketClient();
            WsClient.getReceiveWebsocketClient();

            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                logger.warn("path is empty");
                return;
            }
            URL url = new URL(path);
            logger.info("data path:" + path);
            // fetch the data source
            HttpURLConnection httpConnection =
                    (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            InputStream input = httpConnection.getInputStream();
            ReadableByteChannel channel = Channels.newChannel(input);
            Reader reader = Channels.newReader(channel, "UTF8");

            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Constants.INPUT_BUFFER_SIZE);
            CharBuffer charBuffer = byteBuffer.asCharBuffer();
            char[] chars = new char[Constants.INPUT_BUFFER_SIZE];

            TraceCacheBucket traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
            Set<String> tmpBadTraceIdSet = ERR_TRACE_SET_LIST.get(bucketPos);
            Map<String, Set<String>> traceMap = traceCacheBucket.getData();

            // marked as a working bucket
            traceCacheBucket.tryEnter();
            // use block read
            StringBuilder lineBuilder = new StringBuilder();
            while (reader.read(charBuffer) != -1) {
                charBuffer.flip();
                int remain = charBuffer.remaining();
                charBuffer.get(chars, 0, remain);
                charBuffer.clear();

                for (int i = 0; i < remain; i++) {
                    if (chars[i] == '\n') {
                        lineCount += 1;
                        pos = (int) lineCount / Constants.BUCKET_SIZE;
                        bucketPos = pos % Constants.CLIENT_BUCKET_COUNT;

                        String line = lineBuilder.toString();
                        HANDLER_THREAD_POOL.execute(new Worker(line, lineCount, pos, tmpBadTraceIdSet, traceMap));
                        lineBuilder.delete(0, lineBuilder.length());

                        // some switch operations is also should be done
                        if (lineCount % Constants.BUCKET_SIZE == 0) {
                            // set current bucket as idle status
                            traceCacheBucket.quit();
                            traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
                            tmpBadTraceIdSet = ERR_TRACE_SET_LIST.get(bucketPos);
                            // CAS until enter working status
                            int retryTimes = 0;
                            while (!traceCacheBucket.tryEnter() && !traceMap.isEmpty()) {
                                Thread.sleep(50);
                                retryTimes += 1;
                                // 要是重试超过100次(5s)，直接强制解除工作状态，清空并上报
                                if (retryTimes >= 100) {
                                    logger.warn("pos {}: bucket 占用时间异常，清空数据并上报 {} 处空数据", pos, pos - Constants.CLIENT_BUCKET_COUNT);
                                    updateWrongTraceId(new HashSet<>(), pos - Constants.CLIENT_BUCKET_COUNT);
                                    traceCacheBucket.clear();
                                    traceCacheBucket.quit();
                                }
                            }
                            traceMap = traceCacheBucket.getData();
                        }
                        // ignore LF
                        continue;
                    }
                    lineBuilder.append(chars[i]);
                }
            }
            // last update, clear the badTraceIdSet
            updateWrongTraceId(tmpBadTraceIdSet, pos);
            traceCacheBucket.quit();

            input.close();
            callFinish();
        } catch (Exception e) {
            logger.warn("拉取数据流的过程中产生错误！", e);
        }

    }

    /**
     * call backend to update the wrongTraceIdList
     *
     * @param badTraceIdSet
     * @param pos
     */
    private void updateWrongTraceId(Set<String> badTraceIdSet, int pos) {
        // TODO ConcurrentModificationException
        String json = JSON.toJSONString(badTraceIdSet);
        if (badTraceIdSet.size() > 0) {
            // send badTraceIdList and its pos to the backend
            String msg = String.format("{\"type\": %d, \"badTraceIdSet\": %s, \"pos\": %d}"
                    , Constants.UPDATE_TYPE, json, pos);
            sendWebSocket.sendTextFrame(msg);
            logger.info("成功上报pos {} 的wrongTraceId...", pos);
        }
        // auto clear after update
        badTraceIdSet.clear();
    }

    /**
     * 给定区间中的所有错误traceId和pos，拉取对应traceIds的spans
     *
     * @param wrongTraceIdSet
     * @param pos
     * @return
     */
    public static String getWrongTracing(Set<String> wrongTraceIdSet, int pos) {
        int bucketCount = Constants.CLIENT_BUCKET_COUNT;
        // calculate the three continue pos
        int curr = pos % bucketCount;
        int prev = (curr - 1 == -1) ? bucketCount - 1 : (curr - 1) % bucketCount;
        int next = (curr + 1 == bucketCount) ? 0 : (curr + 1) % bucketCount;

        logger.info(String.format("pos: %d, 开始收集 trace details curr: %d, prev: %d, next: %d，三个 bucket中的数据", pos, curr, prev, next));

        Map<String, Set<String>> wrongTraceMap = new HashMap<>(32);

        // these traceId data should be collect
        getWrongTraceWithBucketPos(prev, wrongTraceIdSet, wrongTraceMap, true);
        getWrongTraceWithBucketPos(curr, wrongTraceIdSet, wrongTraceMap, false);
        getWrongTraceWithBucketPos(next, wrongTraceIdSet, wrongTraceMap, false);

        return JSON.toJSONString(wrongTraceMap);
    }

    /**
     * @param bucketPos
     * @param traceIdSet
     * @param wrongTraceMap
     * @param shouldClear
     */
    private static void getWrongTraceWithBucketPos(int bucketPos, Set<String> traceIdSet, Map<String,
            Set<String>> wrongTraceMap, boolean shouldClear) {
        // backend start pull these bucket
        TraceCacheBucket traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
        // when this bucket is still working, cas until it become idle status, and then set as working status
        while (!traceCacheBucket.tryEnter()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Map<String, Set<String>> traceMap = traceCacheBucket.getData();
        for (String traceId : traceIdSet) {
            Set<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross two bucket (e.g bucket size 20000, span1 in line 19999,
                // span2
                //in line 20001)
                Set<String> existSpanList = wrongTraceMap.get(traceId);
                if (existSpanList != null) {
                    // TODO caused, fixed waited，这里的报错的原因应该是应该是
                    existSpanList.addAll(spanList);
                } else {
                    wrongTraceMap.put(traceId, spanList);
                }
            }
        }
        // the previous bucket must have been consumed, so free this bucket
        if (shouldClear) traceCacheBucket.clear();
        traceCacheBucket.quit();
    }

    private void callFinish() {
        sendWebSocket.sendTextFrame(String.format("{\"type\": %d}", Constants.FIN_TYPE));
        logger.info("已发送 FIN 请求");
    }

    private String getPath() {
        String port = System.getProperty("server.port", "8080");
        // TODO 生产环境切换端口
        if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
            // return "http://localhost:8080/trace1.data";
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
            // return "http://localhost:8080/trace2.data";
        } else {
            return null;
        }
    }

    private class Worker implements Runnable {
        private final String line;
        private final long count;
        private final int pos;
        private final Set<String> tmpBadTraceIdSet;
        private final Map<String, Set<String>> traceMap;

        public Worker(final String line, final long count, final int pos,
                      final Set<String> tmpBadTraceIdSet, final Map<String, Set<String>> traceMap) {
            this.line = line;
            this.count = count;
            this.pos = pos;
            this.tmpBadTraceIdSet = tmpBadTraceIdSet;
            this.traceMap = traceMap;
        }

        @Override
        public void run() {
            StringBuilder traceIdBuilder = new StringBuilder();
            StringBuilder tagsBuilder = new StringBuilder();
            char[] chars = line.toCharArray();
            int ICount = 0;
            for (char tmp : chars) {
                if (tmp == '|') {
                    ICount += 1;
                    continue;
                }
                if (ICount == 0) traceIdBuilder.append(tmp);
                if (ICount == 8) tagsBuilder.append(tmp);
            }
            String traceId = traceIdBuilder.toString();
            String tags = tagsBuilder.toString();

            Set<String> spanList = traceMap.computeIfAbsent(traceId, k -> new HashSet<>());
            spanList.add(line);

            if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
                tmpBadTraceIdSet.add(traceId);
            }

            if (count % Constants.BUCKET_SIZE == 0) {
                // 更新wrongTraceId到backend
                updateWrongTraceId(tmpBadTraceIdSet, pos - 1);
            }
        }
    }
}