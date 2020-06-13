package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.BasicHttpHandler;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.concurrent.*;

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
    // 大桶的 pos 不取余，每 Constants.UPDATE_INTERVAL 行，切换大桶
    private static volatile int pos = 0;
    // 小桶的 pos ，不需要取余, MAX < Constants.CLIENT_SMALL_BUCKET_SIZE, 、
    // 每读 Constants.SWITCH_SMALL_BUCKET_INTERVAL 行，切换小桶
    private static volatile int innerPos = 0;
    // pos % BIG_BUCKET_COUNT
    private static volatile int bigBucketPos = 0;
    private static volatile BigBucket bigBucket;
    private static volatile SmallBucket smallBucket;
    // 一个工作周期中记录所有errTraceId
    private static volatile Set<String> errTraceIdSet;

    private static final StringBuilder lineBuilder = new StringBuilder();
    private static volatile boolean firstSetPriority = true;
    private static Semaphore semaphore = new Semaphore(Constants.SEMAPHORE_SIZE);

    public static void init() {
        for (int i = 0; i < Constants.CLIENT_BIG_BUCKET_COUNT; i++) {
            BigBucket bigBucket = new BigBucket();
            bigBucket.init();
            BUCKET_TRACE_LIST.add(bigBucket);
        }
        HANDLER_THREAD_POOL = new ThreadPoolExecutor(1, 1, 30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new DefaultThreadFactory("line-handler"),
                new ThreadPoolExecutor.AbortPolicy());
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

            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Constants.INPUT_BUFFER_SIZE);

            bigBucket = BUCKET_TRACE_LIST.get(bigBucketPos);
            // errTraceIdSet = ERR_TRACE_SET_LIST.get(bucketPos);
            errTraceIdSet = Sets.newHashSet();
            smallBucket = bigBucket.getSmallBucket(innerPos);

            byte[] bytes;
            int times = 0;
            // marked as a working small bucket
            smallBucket.tryEnter(1, 5, pos, innerPos);
            // use block read
            while (channel.read(byteBuffer) != -1) {
                ((Buffer) byteBuffer).flip();
                int remain = byteBuffer.remaining();
                bytes = new byte[remain];
                byteBuffer.get(bytes, 0, remain);
                ((Buffer) byteBuffer).clear();
                while (!semaphore.tryAcquire(1, TimeUnit.MILLISECONDS)) {
                    Thread.sleep(20);
                }
                times += 1;
                // 让出时间片，让处理线程去处理字节
                if (times % 1600 == 0) {
                    logger.info("queue size {}", ((ThreadPoolExecutor) HANDLER_THREAD_POOL).getQueue().size());
                    // Thread.sleep(10);
                }
                HANDLER_THREAD_POOL.execute(new BlockWorker(bytes, remain));
            }
            // last update, clear the badTraceIdSet
            HANDLER_THREAD_POOL.execute(() -> updateWrongTraceId(errTraceIdSet, pos));
            HANDLER_THREAD_POOL.execute(() -> smallBucket.quit());
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
        int bucketCount = Constants.CLIENT_BIG_BUCKET_COUNT;
        // calculate the three continue pos
        int curr = pos % bucketCount;
        int prev = (curr - 1 == -1) ? bucketCount - 1 : (curr - 1) % bucketCount;
        int next = (curr + 1 == bucketCount) ? 0 : (curr + 1) % bucketCount;

        logger.info(String.format("pos: %d, 开始收集 trace details curr: %d, prev: %d, next: %d，三个 " +
                "bucket中的数据", pos, curr, prev, next));

        // TODO 隐患 LIST SET
        Map<String, List<String>> wrongTraceMap = new HashMap<>(32);

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
    private static void getWrongTraceWithBucketPos(int bucketPos, int pos, Set<String> traceIdSet
            , Map<String,
            List<String>> wrongTraceMap, boolean shouldClear) {
        // backend start pull these bucket
        BigBucket bigBucket = BUCKET_TRACE_LIST.get(bucketPos);
        List<SmallBucket> smallBucketList = bigBucket.getSmallBucketList();
        for (SmallBucket smallBucket : smallBucketList) {
            if (smallBucket.tryEnter(50, 10, pos, innerPos)) {
                // 这里看起来像是O(n^2)的操作，但是实际上traceIdSet的大小基本都非常小
                traceIdSet.forEach(traceId -> {
                    List<String> spans = smallBucket.getSpans(traceId);
                    if (spans != null) {
                        // 这里放入的时候其实也要注意
                        List<String> existSpanList = wrongTraceMap.computeIfAbsent(traceId, k -> new ArrayList<>());
                        existSpanList.addAll(spans);
                    }
                });
            }
            if (shouldClear) smallBucket.clear();
            smallBucket.quit();
        }
    }

    private void callFinish() {
        websocket.sendTextFrame(String.format("{\"type\": %d}", Constants.FIN_TYPE));
        logger.info("已发送 FIN 请求");
    }

    private String getPath() {
        String port = System.getProperty("server.port", "8080");
        String env = System.getProperty("server.env", "prod");
        if ("prod".equals(env)) {
            if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
                return "http://localhost:" + BasicHttpHandler.getDataSourcePort() + "/trace1.data";
            } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
                return "http://localhost:" + BasicHttpHandler.getDataSourcePort() + "/trace2.data";
            } else {
                return null;
            }
        } else {
            if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
                return "http://localhost:8080/trace1.data";
            } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
                return "http://localhost:8080/trace2.data";
            } else {
                return null;
            }
        }
    }

    /**
     * 处理读取出来的一块数据，作为一个worker提交到一个单线程池的任务队列中
     */
    private class BlockWorker implements Runnable {
        char[] chars;
        int readableSize;
        byte[] bytes;

        public BlockWorker(byte[] bytes, int readableSize) {
            this.bytes = bytes;
            // TODO 创建该对象时这个耗时在处理线程中还是IO线程中？
            // this.chars = new String(bytes).toCharArray();
            this.readableSize = readableSize;
        }

        @Override
        public void run() {
            // TODO 设置线程优先级?
            if (firstSetPriority) {
                Thread.currentThread().setPriority(10);
                firstSetPriority = false;
            }
            chars = new String(bytes).toCharArray();
            // 释放引用
            bytes = null;
            int len = chars.length;
            int preBlockPos = 0;
            int blockPos = -1;
            StringBuilder traceIdBuilder = new StringBuilder();
            StringBuilder tagsBuilder = new StringBuilder();

            for (int i = 0; i < len; i++) {
                if (chars[i] == '\n') {
                    preBlockPos = blockPos + 1;
                    blockPos = i;

                    lineBuilder.append(chars, preBlockPos, blockPos - preBlockPos);
                    lineCount += 1;
                    String line = lineBuilder.toString();
                    handleLine(line, traceIdBuilder, tagsBuilder);
                    traceIdBuilder.delete(0, traceIdBuilder.length());
                    tagsBuilder.delete(0, tagsBuilder.length());
                    lineBuilder.delete(0, lineBuilder.length());

                    // 先切大桶，再切小桶
                    if (lineCount % Constants.UPDATE_INTERVAL == 0) {
                        // 更新错误链路id到backend
                        updateWrongTraceId(errTraceIdSet, pos);

                        pos = (int) lineCount / Constants.UPDATE_INTERVAL;
                        bigBucketPos = pos % Constants.CLIENT_BIG_BUCKET_COUNT;
                        // switch to next big bucket
                        bigBucket = BUCKET_TRACE_LIST.get(bigBucketPos);
                    }

                    // 在这里切换小桶，旧桶退出工作状态，新桶进入工作状态
                    if (lineCount % Constants.SWITCH_SMALL_BUCKET_INTERVAL == 0) {
                        smallBucket.quit();
                        if (innerPos == Constants.CLIENT_SMALL_BUCKET_COUNT - 1) {
                            innerPos = 0;
                        } else {
                            innerPos += 1;
                        }
                        smallBucket = bigBucket.getSmallBucket(innerPos);
                        if (!smallBucket.tryEnter(100, 10, pos, innerPos)) {
                            smallBucket.clear();
                            smallBucket.forceEnter();
                            logger.warn("强制清空 pos {} innerPos {} 处的数据", pos, innerPos);
                        }
                    }
                }
            }
            if (len - 1 >= blockPos + 1) {
                lineBuilder.append(chars, blockPos + 1, len - blockPos - 1);
            }
            // 任务结束，释放计数
            semaphore.release();
        }

        public void handleLine(String line, StringBuilder traceIdBuilder, StringBuilder tagsBuilder) {
            char[] lineChars = line.toCharArray();
            int len = lineChars.length;
            // 由于只需要第一部分和最后一部分，所以这样从头找和从结尾找会快很多
            for (int i = 0; i < len; i++) {
                if (lineChars[i] == '|') {
                    traceIdBuilder.append(lineChars, 0, i);
                    break;
                }
            }

            for (int i = len - 1; i >= 0; i--) {
                if (lineChars[i] == '|') {
                    tagsBuilder.append(lineChars, i + 1, len - (i + 1));
                    break;
                }
            }

            String traceId = traceIdBuilder.toString();
            String tags = tagsBuilder.toString();

            // TODO 这里可不可以用list
            List<String> spanList = smallBucket.computeIfAbsent(traceId);
            spanList.add(line);

            if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
                errTraceIdSet.add(traceId);
            }
        }
    }
}