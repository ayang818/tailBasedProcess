package com.ayang818.middleware.tailbase.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
public class SmallBucket {
    private final AtomicBoolean isWorking;
    // key is traceId, value is all spans
    private final Map<String, List<byte[]>> data;
    private static final Logger logger = LoggerFactory.getLogger(SmallBucket.class);

    public SmallBucket(int size) {
        isWorking = new AtomicBoolean(false);
        data = new ConcurrentHashMap<>(size);
    }

    public List<byte[]> getSpans(String traceId) {
        return data.get(traceId);
    }

    /**
     * @param retryTimes 重试次数
     * @param sleepTime 重试睡眠时间
     * @return true means success
     */
    public boolean tryEnter(int retryTimes, int sleepTime, int pos, int innerPos) {
        int i = 0;
        while (i < retryTimes) {
            i += 1;
            // logger.info("等待进入 pos {} innerPos {}", pos, innerPos);
            if (this.isWorking.compareAndSet(false, true)) {
                return true;
            } else {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public void forceEnter() {
        this.isWorking.set(true);
    }

    public void quit() {
        this.isWorking.compareAndSet(true, false);
    }

    public List<byte[]> computeIfAbsent(String traceId) {
        return data.computeIfAbsent(traceId, k -> new ArrayList<>());
    }

    public void clear() {
        data.clear();
    }
}
