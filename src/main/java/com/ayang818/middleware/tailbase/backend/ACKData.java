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
        String traceId;
        List<String> spans;
        List<String> newSpans;
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            traceId = entry.getKey();
            spans = ackMap.get(traceId);
            newSpans = entry.getValue();
            if (spans == null) {
                ackMap.put(traceId, newSpans);
            } else {
                spans.addAll(newSpans);
            }
        }
        return remainAccessTime.decrementAndGet();
    }

    public Map<String, List<String>> getAckMap() {
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
