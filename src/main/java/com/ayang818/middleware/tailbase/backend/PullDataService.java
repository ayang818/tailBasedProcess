package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.ayang818.middleware.tailbase.CommonController;
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

    public static Map<String, String> resMap = new ConcurrentHashMap<>();

    public static LinkedBlockingQueue<TraceIdBucket> blockingQueue =
            new LinkedBlockingQueue<>(100);

    private int prePos = -1;

    private static final ExecutorService START_POOL = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10), new DefaultThreadFactory("backend-starter"));

    public static void start() {
        START_POOL.execute(new PullDataService());
    }

    private static int timer = 0;

    @Override
    public void run() {
        while (true) {
            TraceIdBucket bucket = null;
            try {
                // 0.5 秒没有得到新的可以消费的数据，检查是否结束
                bucket = blockingQueue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (bucket == null) {
                // 考虑是否已经全部消费完了
                if (MessageHandler.isFin()) {
                    if (sendCheckSum()) {
                        break;
                    }
                }
                timer += 1;
                if (timer >= 100) {
                    // 如果重试次数超过100次，结束评测，免得等很长时间
                    sendCheckSum();
                }
                logger.info("失败获取可消费的bucket，上次消费成功的pos为 {} ......", prePos);
                continue;
            }
            // 获取到了bucket，timer置为0
            timer = 0;
            prePos = bucket.getPos();
            // 发送取到的errTraceId 和 对应的 pos
            Set<String> badTraceIdSet = bucket.getTraceIdSet();
            int pos = bucket.getPos();

            if (!badTraceIdSet.isEmpty()) {
                // pull data from each client, then MessageHandler will consume these data
                MessageHandler.pullWrongTraceDetails(JSON.toJSONString(badTraceIdSet), pos);
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
