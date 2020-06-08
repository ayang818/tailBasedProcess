package com.ayang818.middleware.tailbase.backend;

import com.ayang818.middleware.tailbase.Constants;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author 杨丰畅
 * @description 暂存错误的traceId，用于向client拉取具体数据
 * @date 2020/5/22 23:21
 **/
public class TraceIdBucket {
    private int pos = -1;
    private int processCount = 0;
    // 一个bucket中20条traceId
    private Set<String> traceIdSet = new HashSet<>(Constants.BUCKET_ERR_TRACE_COUNT);

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getProcessCount() {
        return processCount;
    }

    public Set<String> getTraceIdSet() {
        return traceIdSet;
    }

    public synchronized int addProcessCount() {
        processCount += 1;
        return processCount;
    }

    public void reset() {
        pos = -1;
        processCount = 0;
        traceIdSet.clear();
    }
}
