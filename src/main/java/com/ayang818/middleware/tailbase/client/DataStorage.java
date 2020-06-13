package com.ayang818.middleware.tailbase.client;

import com.ayang818.middleware.tailbase.Constants;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author 杨丰畅
 * @description client 存储部分公共数据的对象
 * @date 2020/5/22 22:00
 **/
public class DataStorage {
    public static List<BigBucket> BUCKET_TRACE_LIST = new ArrayList<>(Constants.CLIENT_BIG_BUCKET_COUNT);

    public static final ExecutorService START_POOL = new ThreadPoolExecutor(1, 1, 60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10), new DefaultThreadFactory("client_starter"));

    public static ExecutorService HANDLER_THREAD_POOL;
}
