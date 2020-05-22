package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.CommonController;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
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
import java.util.concurrent.ConcurrentHashMap;

import static com.ayang818.middleware.tailbase.client.DataStorage.BUCKET_TRACE_LIST;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:47
 **/
@Service
public class ClientDataStreamHandler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ClientDataStreamHandler.class);

    private static int BUCKET_COUNT = 20;

    private static WebSocket wsClient = WsClient.getWebSocketClient();

    public static  void init() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            BUCKET_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BUCKET_SIZE));
        }
    }

    @Override
    public void run() {
        try {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                logger.warn("path is empty");
                return ;
            }
            URL url = new URL(path);
            logger.info("data path:" + path);
            // fetch the data source
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            InputStream input = httpConnection.getInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(input));

            String line;
            long count = 0;
            int pos = 0;

            Set<String> tmpBadTraceIdSet = new HashSet<>(1000);
            Map<String, List<String>> traceMap = BUCKET_TRACE_LIST.get(pos);

            // start read stream line by line
            while ((line = bf.readLine()) != null) {
                count++;
                String[] cols = line.split("\\|");
                if (cols.length < 9) {
                    logger.info("bad span format");
                    continue;
                }
                String traceId = cols[0];
                String tags = cols[8];

                List<String> spanList = traceMap.computeIfAbsent(traceId, k -> new ArrayList<>());
                spanList.add(line);

                if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
                    tmpBadTraceIdSet.add(traceId);
                }

                // replace with next bucket
                if (count % Constants.BUCKET_SIZE == 0) {
                    pos++;
                    // loop cycle
                    if (pos >= BUCKET_COUNT) {
                        pos = 0;
                    }
                    traceMap = BUCKET_TRACE_LIST.get(pos);
                    // donot produce data, wait backend to consume data
                    // TODO to use lock/notify
                    while (!traceMap.isEmpty()) {
                        Thread.sleep(10);
                    }
                    // bucketPos begin from 0, so need to minus 1
                    int bucketPos = (int) count / Constants.BUCKET_SIZE - 1;

                    updateWrongTraceId(tmpBadTraceIdSet, bucketPos);
                }
            }
            // last update, clear the badTraceIdSet
            updateWrongTraceId(tmpBadTraceIdSet, (int) (count / Constants.BUCKET_SIZE - 1));
            bf.close();
            input.close();
            callFinish();
        } catch (Exception e) {
            logger.warn("fail to process data", e);
        }

    }

    /**
     * call backend to update the wrongTraceIdList
     * @param badTraceIdSet
     * @param bucketPos
     */
    private void updateWrongTraceId(Set<String> badTraceIdSet, int bucketPos) {
        String json = JSON.toJSONString(badTraceIdSet);
        if (badTraceIdSet.size() > 0) {
            // send badTraceIdList and its pos to the backend
            String msg = String.format("{\"type\": %d, \"badTraceIdSet\": %s, \"bucketPos\": %d}"
                    , Constants.UPDATE_TYPE, json, bucketPos);
            wsClient.sendTextFrame(msg);
        }
        badTraceIdSet.clear();
        logger.info("success update current badTraceIdSet to the backend....");
    }

    /**
     * 给定 区间中的所有错误traceId和便宜量，拉取对应traceIds的spans
     * @param wrongTraceIdList
     * @param bucketPos
     * @return
     */
    public static String getWrongTracing(List<String> wrongTraceIdList, int bucketPos) {
        // calculate the three continue pos
        int pos = bucketPos % BUCKET_COUNT;
        int previous = pos - 1;
        int next = pos + 1;

        if (previous == -1) {
            previous = BUCKET_COUNT -1;
        }
        if (next == BUCKET_COUNT) {
            next = 0;
        }

        Map<String, List<String>> wrongTraceMap = new HashMap<>(32);
        // these traceId data should be collect

        getWrongTraceWithBucketPos(previous, pos, wrongTraceIdList, wrongTraceMap);
        getWrongTraceWithBucketPos(pos, pos, wrongTraceIdList,  wrongTraceMap);
        getWrongTraceWithBucketPos(next, pos, wrongTraceIdList, wrongTraceMap);

        // TODO to use lock/notify, get the lock of this bucket
        // the previous bucket must have been consumed, so free this bucket
        BUCKET_TRACE_LIST.get(previous).clear();

        return JSON.toJSONString(wrongTraceMap);
    }

    /**
     * @param bucketPos
     * @param pos
     * @param traceIdList
     * @param wrongTraceMap
     */
    private static void getWrongTraceWithBucketPos(int bucketPos, int pos, List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
        // backend start pull these bucket
        Map<String, List<String>> traceMap = BUCKET_TRACE_LIST.get(bucketPos);

        for (String traceId : traceIdList) {
            List<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross two bucket (e.g bucket size 20000, span1 in line 19999,
                //span2
                //in line 20001)
                List<String> existSpanList = wrongTraceMap.get(traceId);
                if (existSpanList != null) {
                    existSpanList.addAll(spanList);
                } else {
                    wrongTraceMap.put(traceId, spanList);
                }
                logger.info(String.format("getWrongTracing, bucketPos:%d, pos:%d, traceId:%s, " +
                                "spanListSize:\n %d",
                        bucketPos, pos,  traceId, spanList.size()));
            }
        }
    }

    private void callFinish() {

    }

    private String getPath(){
        String port = System.getProperty("server.port", "8080");
        if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)){
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
        } else {
            return null;
        }
    }
}