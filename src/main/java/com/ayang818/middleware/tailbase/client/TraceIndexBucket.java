package com.ayang818.middleware.tailbase.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 小桶
 * <p>进入工作状态有以下几种可能<br>
 * 1. 开始往此bucket中添加数据<br>
 * 2. 开始消费此bucket中的数据<br>
 * 结束工作状态同样也就是上述两种状态的结束
 * 在开始进入工作状态前，需要判断是否处于工作状态 ? 等待转化为非工作状态 : 直接进入</p>
 *
 * @author : chengyi
 * @date : 2020-06-13 20:34
 **/
public class TraceIndexBucket {
    private final AtomicBoolean isWorking;
    // key is traceId, value is all spans
    private final Map<String, List<int[]>> indexes;
    private static final Logger logger = LoggerFactory.getLogger(TraceIndexBucket.class);

    public TraceIndexBucket(int size) {
        isWorking = new AtomicBoolean(false);
        indexes = new HashMap<>(size);
    }

    public List<int[]> getSpansIndex(String traceId) {
        return indexes.get(traceId);
    }

    /**
     * 这里好像很难调，暂时变成阻塞的吧
     * @return true means success
     */
    public boolean tryEnter() {
        while (!this.isWorking.compareAndSet(false, true)) {}
        return true;
    }

    public void forceEnter() {
        this.isWorking.set(true);
    }

    public void quit() {
        this.isWorking.compareAndSet(true, false);
    }

    public List<int[]> computeIfAbsent(String traceId) {
        return indexes.computeIfAbsent(traceId, k -> new ArrayList<>());
    }

    public void clear() {
        indexes.clear();
    }

    public List<int[]> get(String traceId) {
        return indexes.get(traceId);
    }

    public void put(String traceId, List<int[]> spanList) {
        indexes.put(traceId, spanList);
    }
}
