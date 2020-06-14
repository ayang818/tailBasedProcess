package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.BasicHttpHandler;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import io.netty.util.concurrent.DefaultThreadFactory;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public static void start() {
        START_POOL.execute(new PullDataService());
    }

    private static int timer = 0;

    @Override
    public void run() {
        try {
            while (BasicHttpHandler.getDataSourcePort() == null) {}
            while (true) {
                TraceIdBucket bucket = null;

                // 0.5 秒没有得到新的可以消费的数据，检查是否结束
                bucket = blockingQueue.poll(5, TimeUnit.MILLISECONDS);

                if (bucket == null) {
                    // 考虑是否已经全部消费完了
                    if (MessageHandler.isFin()) {
                        if (sendCheckSum()) {
                            break;
                        }
                    }
                    timer += 1;
                    Thread.sleep(15);
                    if (timer >= 100) {
                        // 如果重试次数超过2s，说明程序可能有问题，结束评测，免得等很长时间
                        logger.info("重试时间超过4s，直接发送checkSum");
                        sendCheckSum();
                        break;
                    }
                    continue;
                }
                // 获取到了bucket，timer置为0
                timer = 0;
                // 发送取到的errTraceId 和 对应的 pos
                Set<String> badTraceIdSet = bucket.getTraceIdSet();
                int pos = bucket.getPos();

                if (!badTraceIdSet.isEmpty()) {
                    // pull data from each client, then MessageHandler will consume these data
                    MessageHandler.pullWrongTraceDetails(JSON.toJSONString(badTraceIdSet), pos);
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
            String url = String.format("http://localhost:%s/api/finished",
                    BasicHttpHandler.getDataSourcePort());
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
