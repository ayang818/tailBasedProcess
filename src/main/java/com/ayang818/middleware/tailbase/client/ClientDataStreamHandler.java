package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.BasicHttpHandler;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.ayang818.middleware.tailbase.client.DataStorage.*;

/**
 * @author 杨丰畅
 * @description client 端用于处理读入数据流，向 backend 发送对应请求的类
 * @date 2020/5/5 12:47
 **/
public class ClientDataStreamHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ClientDataStreamHandler.class);
    public static WebSocket websocket;
    // 行号
    private static volatile long lineCount = 0;
    // 第 pos 轮， 不取余
    private static volatile int pos = 0;
    // bucketPos 在bucketList中的位置，pos % BUCKET_COUNT
    private static volatile int bucketPos = 0;
    // bucket list中取到的一个即将进入working state的桶
    private static volatile TraceCacheBucket traceCacheBucket;
    // 一个工作周期中记录所有errTraceId
    private static volatile Set<String> errTraceIdSet;
    private static volatile Map<String, Set<String>> traceMap;
    private static final StringBuilder lineBuilder = new StringBuilder();

    public static void init() {
        for (int i = 0; i < Constants.CLIENT_BUCKET_COUNT; i++) {
            BUCKET_TRACE_LIST.add(new TraceCacheBucket(Constants.CLIENT_BUCKET_MAP_SIZE));
            ERR_TRACE_SET_LIST.add(new HashSet<>(Constants.BUCKET_ERR_TRACE_COUNT));
        }
        HANDLER_THREAD_POOL = new ThreadPoolExecutor(1, 1, 60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(500000),
                new DefaultThreadFactory("line-handler"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static void start() {
        START_POOL.execute(new ClientDataStreamHandler());
    }

    @Override
    public void run() {
        try {
            websocket = WsClient.getWebsocketClient();

            String path = getPath();
            // process data on client, not server
            if (path == null || "".equals(path)) {
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

            traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
            errTraceIdSet = ERR_TRACE_SET_LIST.get(bucketPos);
            traceMap = traceCacheBucket.getData();
            char[] chars;

            // marked as a working bucket
            traceCacheBucket.tryEnter();
            // use block read
            while (reader.read(charBuffer) != -1) {
                ((Buffer) charBuffer).flip();
                int remain = charBuffer.remaining();
                chars = new char[remain];
                charBuffer.get(chars, 0, remain);
                ((Buffer) charBuffer).clear();
                HANDLER_THREAD_POOL.execute(new BlockWorker(chars, remain));
            }
            // last update, clear the badTraceIdSet
            HANDLER_THREAD_POOL.execute(() -> updateWrongTraceId(errTraceIdSet, pos));
            HANDLER_THREAD_POOL.execute(() -> traceCacheBucket.quit());
            HANDLER_THREAD_POOL.execute(this::callFinish);
            input.close();
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
        String json = JSON.toJSONString(badTraceIdSet);
        if (badTraceIdSet.size() > 0) {
            // send badTraceIdList and its pos to the backend
            String msg = String.format("{\"type\": %d, \"badTraceIdSet\": %s, \"pos\": %d}"
                    , Constants.UPDATE_TYPE, json, pos);
            websocket.sendTextFrame(msg);
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
        getWrongTraceWithBucketPos(prev, pos, wrongTraceIdSet, wrongTraceMap, true);
        getWrongTraceWithBucketPos(curr, pos, wrongTraceIdSet, wrongTraceMap, false);
        getWrongTraceWithBucketPos(next, pos, wrongTraceIdSet, wrongTraceMap, false);

        return JSON.toJSONString(wrongTraceMap);
    }

    /**
     * @param bucketPos
     * @param traceIdSet
     * @param wrongTraceMap
     * @param shouldClear
     */
    private static void getWrongTraceWithBucketPos(int bucketPos, int pos, Set<String> traceIdSet, Map<String,
            Set<String>> wrongTraceMap, boolean shouldClear) {
        // backend start pull these bucket
        TraceCacheBucket traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
        // when this bucket is still working, cas until it become idle status, and then set as working status
        while (!traceCacheBucket.tryEnter()) {
            try {
                Thread.sleep(10);
                logger.info("等待进入bucket {}", pos);
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
        websocket.sendTextFrame(String.format("{\"type\": %d}", Constants.FIN_TYPE));
        logger.info("已发送 FIN 请求");
    }

    private String getPath() {
        String port = System.getProperty("server.port", "8080");
        // TODO 生产环境切换端口
        if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
            // return "http://localhost:8080/trace1.data";
            return "http://localhost:" + BasicHttpHandler.getDataSourcePort() + "/trace1.data";
        } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
            return "http://localhost:" + BasicHttpHandler.getDataSourcePort() + "/trace2.data";
            // return "http://localhost:8080/trace2.data";
        } else {
            return null;
        }
    }

    /**
     * 处理读取出来的一块数据，作为一个worker提交到一个单线程池的任务队列中
     */
    private class BlockWorker implements Runnable {
        char[] chars;
        int readableSize;

        public BlockWorker(char[] chars, int readableSize) {
            this.chars = chars;
            this.readableSize = readableSize;
        }

        @Override
        public void run() {
            int preBlockPos = 0;
            int blockPos = -1;
            for (int i = 0; i < readableSize; i++) {
                if (chars[i] == '\n') {
                    preBlockPos = blockPos + 1;
                    blockPos = i;
                    lineBuilder.append(chars, preBlockPos, blockPos - preBlockPos);
                    lineCount += 1;
                    String line = lineBuilder.toString();
                    handleLine(line);
                    lineBuilder.delete(0, lineBuilder.length());

                    // some switch operations is also should be done
                    if (lineCount % Constants.BUCKET_SIZE == 0) {
                        // set current bucket as idle status
                        traceCacheBucket.quit();
                        // switch to next
                        traceCacheBucket = BUCKET_TRACE_LIST.get(bucketPos);
                        errTraceIdSet = ERR_TRACE_SET_LIST.get(bucketPos);
                        // CAS until enter working status
                        int retryTimes = 0;
                        while (!traceCacheBucket.tryEnter() && !traceMap.isEmpty()) {
                            try {
                                logger.info("无法进行下一工作 pos {}", pos);
                                Thread.sleep(25);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            retryTimes += 1;
                            // 要是重试超过100次(5s)，直接强制解除工作状态，清空并上报
                            if (retryTimes >= 200) {
                                logger.warn("pos {}: bucket 占用时间异常，清空数据并上报 {} 处空数据", pos, pos - Constants.CLIENT_BUCKET_COUNT);
                                updateWrongTraceId(new HashSet<>(), pos - Constants.CLIENT_BUCKET_COUNT);
                                traceCacheBucket.clear();
                                traceCacheBucket.quit();
                            }
                        }
                        traceMap = traceCacheBucket.getData();
                    }
                }
            }
            if (readableSize - 1 >= blockPos + 1) {
                lineBuilder.append(chars, blockPos + 1, readableSize - blockPos - 1);
            }
        }

        public void handleLine(String line) {
            StringBuilder traceIdBuilder = new StringBuilder();
            StringBuilder tagsBuilder = new StringBuilder();
            char[] lineChars = line.toCharArray();
            int ICount = 0;
            int tagsStartPos = 0;
            for (int i = 0; i < lineChars.length; i++) {
                if (lineChars[i] == '|') {
                    ICount += 1;
                    if (ICount == 1) {
                        traceIdBuilder.append(lineChars, 0, i);
                    }
                    if (ICount == 8) {
                        tagsStartPos = i + 1;
                        tagsBuilder.append(lineChars, tagsStartPos, lineChars.length - tagsStartPos);
                        break;
                    }
                }
            }
            String traceId = traceIdBuilder.toString();
            String tags = tagsBuilder.toString();

            Set<String> spanList = traceMap.computeIfAbsent(traceId, k -> new HashSet<>());
            spanList.add(line);

            if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
                errTraceIdSet.add(traceId);
            }

            if (lineCount % Constants.BUCKET_SIZE == 0) {
                // 更新这两个offset
                pos = (int) lineCount / Constants.BUCKET_SIZE;
                bucketPos = pos % Constants.CLIENT_BUCKET_COUNT;

                // 更新wrongTraceId到backend
                updateWrongTraceId(errTraceIdSet, pos - 1);
            }
        }
    }
}