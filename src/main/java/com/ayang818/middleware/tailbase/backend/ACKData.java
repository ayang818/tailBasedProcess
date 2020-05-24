package com.ayang818.middleware.tailbase.backend;

import com.ayang818.middleware.tailbase.Constants;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 杨丰畅
 * @description 用于暂时储存收到的trace detail
 * @date 2020/5/23 14:17
 **/
public class ACKData {
    private AtomicInteger remainAccessTime = new AtomicInteger(Constants.TARGET_PROCESS_COUNT);

    private ConcurrentHashMap<String, List<String>> ackMap = new ConcurrentHashMap<>(32);

    public int putAll(Map<String, List<String>> map) {
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            List<String> spans = ackMap.get(entry.getKey());
            if (spans == null) {
                ackMap.put(entry.getKey(), entry.getValue());
            } else {
                spans.addAll(entry.getValue());
            }
        }
        return remainAccessTime.decrementAndGet();
    }

    public int getRemainAccessTime() {
        return remainAccessTime.get();
    }

    public ConcurrentHashMap<String, List<String>> getAckMap() {
        return ackMap;
    }

    public int size() {
        return ackMap.size();
    }

    public void clear() {
        ackMap.clear();
        remainAccessTime.set(Constants.TARGET_PROCESS_COUNT);
    }
}
