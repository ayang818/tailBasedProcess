package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.CommonController;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

import static com.ayang818.middleware.tailbase.client.DataStorage.*;

/**
 * @author 杨丰畅
 * @description client 端用于处理读入数据流，向 backend 发送对应请求的类
 * @date 2020/5/5 12:47
 **/
@Service
public class ClientDataStreamHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ClientDataStreamHandler.class);

    private static int BUCKET_COUNT = 20;

    private static WebSocket wsClient;

    private static final ExecutorService START_POOL = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10), new DefaultThreadFactory("startPool-client"));

    public static void init() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            BUCKET_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BUCKET_SIZE));
        }
    }

    public static void start() {
        START_POOL.execute(new ClientDataStreamHandler());
    }

    @Override
    public void run() {
        try {
            wsClient = WsClient.getWebSocketClient();
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                logger.warn("path is empty");
                return;
            }
            URL url = new URL(path);
            logger.info("data path:" + path);
            // fetch the data source
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            InputStream input = httpConnection.getInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            String line;
            long count = 0;
            // 第 pos 轮， 不取余
            int pos = 0;
            // bucketPos 在bucketList中的位置，pos % BUCKET_COUNT
            int bucketPos = 0;

            Set<String> tmpBadTraceIdSet = new HashSet<>(1000);
            Map<String, Set<String>> traceMap = BUCKET_TRACE_LIST.get(bucketPos);

            // start read stream line by line
            while ((line = bf.readLine()) != null) {
                count++;
                pos = (int) count / Constants.BUCKET_SIZE;
                bucketPos = pos % BUCKET_COUNT;

                String[] cols = line.split("\\|");
                if (cols.length < 9) {
                    logger.info("bad span format");
                    continue;
                }
                String traceId = cols[0];
                String tags = cols[8];

                Set<String> spanList = traceMap.computeIfAbsent(traceId, k -> new HashSet<>());
                spanList.add(line);

                if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
                    tmpBadTraceIdSet.add(traceId);
                }

                // replace with next bucket
                if (count % Constants.BUCKET_SIZE == 0) {
                    // 更新wrongTraceId到backend
                    updateWrongTraceId(tmpBadTraceIdSet, pos - 1);

                    traceMap = BUCKET_TRACE_LIST.get(bucketPos);
                    // donot produce data, wait backend to consume data
                    // TODO to use lock/notify 其实这里也可以用producer/consumer优化

                    int times = 0;
                    while (!traceMap.isEmpty()) {
                        logger.info("等待 {} 处 bucket 被消费, 当前进行到 {} pos", bucketPos, pos);
                        Thread.sleep(1000);
                        times += 1;
                        // 要是重试超过20次(20s)，直接结束
                        if (times >= 20) {
                            callFinish();
                        }
                    }
                }
            }
            // last update, clear the badTraceIdSet
            updateWrongTraceId(tmpBadTraceIdSet, pos);

            bf.close();
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
        String json = JSON.toJSONString(badTraceIdSet);
        if (badTraceIdSet.size() > 0) {
            // send badTraceIdList and its pos to the backend
            String msg = String.format("{\"type\": %d, \"badTraceIdSet\": %s, \"pos\": %d}"
                    , Constants.UPDATE_TYPE, json, pos);
            wsClient.sendTextFrame(msg);
            logger.info("成功上报wrongTraceId...");
        }
        // 清空2w为区间的set
        badTraceIdSet.clear();
    }

    /**
     * 给定区间中的所有错误traceId和pos，拉取对应traceIds的spans
     *
     * @param wrongTraceIdList
     * @param pos
     * @return
     */
    public static String getWrongTracing(List<String> wrongTraceIdList, int pos) {
        // calculate the three continue pos
        int curr = pos % BUCKET_COUNT;
        int prev = (curr - 1 == -1) ? BUCKET_COUNT - 1 : (curr - 1) % BUCKET_COUNT;
        int next = (curr + 1 == BUCKET_COUNT) ? 0 : (curr + 1) % BUCKET_COUNT;

        logger.info(String.format("开始收集 trace details curr: %d, prev: %d, next: %d，三个 bucket 中的数据", curr, prev, next));

        // a tmp map to collect spans
        Map<String, Set<String>> wrongTraceMap = new ConcurrentHashMap<>(32);

        // these traceId data should be collect
        getWrongTraceWithBucketPos(prev, pos, wrongTraceIdList, wrongTraceMap);
        getWrongTraceWithBucketPos(curr, pos, wrongTraceIdList, wrongTraceMap);
        getWrongTraceWithBucketPos(next, pos, wrongTraceIdList, wrongTraceMap);

        // the previous bucket must have been consumed, so free this bucket
        BUCKET_TRACE_LIST.get(prev).clear();

        return JSON.toJSONString(wrongTraceMap);
    }

    /**
     * @param bucketPos
     * @param pos
     * @param traceIdList
     * @param wrongTraceMap
     */
    private static void getWrongTraceWithBucketPos(int bucketPos, int pos, List<String> traceIdList, Map<String, Set<String>> wrongTraceMap) {
        // backend start pull these bucket
        Map<String, Set<String>> traceMap = BUCKET_TRACE_LIST.get(bucketPos);
        // TODO 这里为什么会出现NPE呢？
        if (traceMap == null) return;
        for (String traceId : traceIdList) {
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
                //logger.info(String.format("拉取错误链路具体内容, bucketPos: %d, pos: %d, traceId: %s, spanListSize: %d",
                //        bucketPos, pos, traceId, spanList.size()));
            }
        }
    }

    private void callFinish() {
        wsClient.sendTextFrame(String.format("{\"type\": %d}", Constants.FIN_TYPE));
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
}