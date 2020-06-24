package com.ayang818.middleware.tailbase.client;

import com.ayang818.middleware.tailbase.Constants;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * @author 杨丰畅
 * @description client 存储部分公共数据的对象
 * @date 2020/5/22 22:00
 **/
public class DataStorage {
    public static List<BigBucket> BUCKET_TRACE_LIST = new ArrayList<>(Constants.CLIENT_BIG_BUCKET_COUNT);
    // 一个pos中对应bucket的所有block数据，一个pos位置的bucket大致有10个block
    public static List<List<byte[]>> DATA_LIST = new ArrayList<>(Constants.CLIENT_BIG_BUCKET_COUNT);

    public static final ExecutorService START_POOL = new ThreadPoolExecutor(1, 1, 60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(10), new DefaultThreadFactory("client-main"));

    public static ExecutorService UPDATE_THREAD = new ThreadPoolExecutor(1, 1, 60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000), new DefaultThreadFactory("update-thread"));

    public static ExecutorService DETAIL_THREAD = new ThreadPoolExecutor(1, 1, 60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(400), new DefaultThreadFactory("detail-thread"));

    public static final Queue<String> updateDataQueue = new LinkedBlockingQueue<>();
}
