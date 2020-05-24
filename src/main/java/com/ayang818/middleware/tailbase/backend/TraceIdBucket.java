package com.ayang818.middleware.tailbase.backend;

import com.ayang818.middleware.tailbase.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 杨丰畅
 * @description 暂存错误的traceId，用于向client拉取具体数据
 * @date 2020/5/22 23:21
 **/
public class TraceIdBucket {
    private int pos = 0;
    private int processCount = 0;
    private List<String> traceIdList = new ArrayList<>(Constants.BUCKET_SIZE / 10);

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getProcessCount() {
        return processCount;
    }

    public List<String> getTraceIdList() {
        return traceIdList;
    }

    public synchronized int addProcessCount() {
        processCount += 1;
        return processCount;
    }

    public void clear() {
        pos = 0;
        processCount = 0;
        traceIdList.clear();
    }
}
