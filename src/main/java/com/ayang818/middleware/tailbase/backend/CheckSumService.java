package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.CommonController;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/22 23:41
 **/
public class CheckSumService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheckSumService.class);

    public static Map<String, String> resMap = new ConcurrentHashMap<>();

    private int prePos = -1;

    public static void start() {
        new Thread(new CheckSumService(), "CheckSumThread").start();
    }

    @Override
    public void run() {
        TraceIdBucket traceIdBucket = null;
        while (true) {
            TraceIdBucket bucket = MessageHandler.getFinishedBucket(prePos + 1);
            if (bucket == null) {
                // 考虑是否已经全部消费完了
                if (MessageHandler.isFin()) {
                    if (sendCheckSum()) {
                        break;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // wait some times
                }
                continue;
            }
            prePos = bucket.getPos();
            // 发送取到的errTraceId 和 对应的 pos
            List<String> traceIdList = bucket.getTraceIdList();
            int pos = bucket.getPos();

            if (!traceIdList.isEmpty()) {
                String traceIdListString = JSON.toJSONString(traceIdList);
                // pull data from each client, then MessageHandler will consume these data
                MessageHandler.pullWrongTraceDetails(traceIdListString, pos);
                bucket.setAsWaiting();
            }
        }
    }
    public static boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(resMap);
            RequestBody body = new FormBody.Builder()
                    .add("result", result).build();
            String url = String.format("http://localhost:%d/api/finished",
                    CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = BaseUtils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                logger.warn("已向评测程序发送checkSum......");
                return true;
            }
            logger.warn("fail to sendCheckSum:" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            logger.warn("fail to call finish", e);
        }
        return false;
    }
}
