package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.BasicHttpHandler;
import com.ayang818.middleware.tailbase.common.Caller;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import io.netty.util.concurrent.DefaultThreadFactory;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author 杨丰畅
 * @description 从阻塞队列中获取消费对象，用于向 client 发出拉起数据请求的线程
 * @date 2020/5/22 23:41
 **/
public class PullDataService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PullDataService.class);

    public static Map<String, String> resMap = new ConcurrentHashMap<>(10240);

    public static LinkedBlockingQueue<TraceIdBucket> blockingQueue =
            new LinkedBlockingQueue<>(100);

    private static final ExecutorService START_POOL = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10), new DefaultThreadFactory("backend-starter"));

    private static int timer = 0;
    private static boolean flag = false;
    private static boolean started = false;

    public static void start() {
        START_POOL.execute(new PullDataService());
    }

    @Override
    public void run() {
        try {
            while (BasicHttpHandler.getDataSourcePort() == null) {}
            while (true) {
                // 0.5 秒没有得到新的可以消费的数据，检查是否结束
                while (true) {
                    if (blockingQueue.size() != 0) {
                        break;
                    }
                    timer += 1;
                    if (MessageHandler.isFin()) {
                        sendCheckSum();
                        flag = true;
                        break;
                    }
                    Thread.sleep(10);
                    // TODO smaller
                    if (timer >= 200 && started) {
                        // 如果重试次数超过5s，说明程序可能有问题，结束评测，免得等很长时间
                        // *info("重试时间超过5s，直接发送checkSum");
                        sendCheckSum();
                        flag = true;
                        break;
                    }
                }
                if (flag) break;
                // 可以bucket，timer置为0
                timer = 0;
                started = true;
                List<Caller.PullDataBucket> traceIdBucketList = new ArrayList<>();
                while (blockingQueue.size() > 0) {
                    TraceIdBucket traceIdBucket = blockingQueue.poll();
                    traceIdBucketList.add(new Caller.PullDataBucket(traceIdBucket.getTraceIdSet(), traceIdBucket.getPos()));
                }
                // pull data from each client, then MessageHandler will consume these data
                if (traceIdBucketList.size() > 0) {
                    MessageHandler.pullWrongTraceDetails(traceIdBucketList);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(resMap);
            RequestBody body = new FormBody.Builder()
                    .add("result", result).build();
            String url = "http://localhost:"+ BasicHttpHandler.getDataSourcePort() +"/api/finished";
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = BaseUtils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                // *warn("已向评测程序发送checkSum......");
                return true;
            }
            // *warn("fail to sendCheckSum:" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            // *warn("fail to call finish", e);
        }
        return false;
    }
}
