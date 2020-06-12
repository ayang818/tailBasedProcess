package com.ayang818.middleware.tailbase.client;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ayang818.middleware.tailbase.Constants.*;

/**
 * 进入工作状态有以下几种可能
 * 1. 开始往此bucket中添加数据
 * 2. 开始消费此bucket中的数据
 * 结束工作状态同样也就是上述两种状态的结束
 * 在开始进入工作状态前，需要判断是否处于工作状态 ? 等待转化为非工作状态 : 直接进入
 */
public class TraceCacheBucket {
    private Map<String, Set<String>> data;
    private final AtomicBoolean isWorking;

    public TraceCacheBucket(int size) {
        data = new ConcurrentHashMap<>(size);
        isWorking = new AtomicBoolean(false);
    }

    public Map<String, Set<String>> getData() {
        return data;
    }

    public boolean isWorking() {
        return isWorking.get();
    }

    public boolean tryEnter() {
        return this.isWorking.compareAndSet(false, true);
    }

    public void quit() {
        this.isWorking.compareAndSet(true, false);
    }

    public void clear() {
        this.data.clear();
    }
}
