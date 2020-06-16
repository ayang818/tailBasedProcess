package com.ayang818.middleware.tailbase.client;

import com.ayang818.middleware.tailbase.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * 大桶，存储一个小桶列表
 */
public class BigBucket {
    List<TraceIndexBucket> traceIndexBuckets;

    public BigBucket() {
        this.traceIndexBuckets = new ArrayList<>(Constants.CLIENT_SMALL_BUCKET_COUNT);
    }

    public TraceIndexBucket getSmallBucket(int innerPos) {
        if (innerPos < 0 || innerPos > Constants.CLIENT_SMALL_BUCKET_COUNT) {
            throw new IndexOutOfBoundsException(String.format("innerPos %d out of bigBucket", innerPos));
        }
        return traceIndexBuckets.get(innerPos);
    }

    public List<TraceIndexBucket> getSmallBucketList() {
        return traceIndexBuckets;
    }

    public void init() {
        for (int i = 0; i < Constants.CLIENT_SMALL_BUCKET_COUNT; i++) {
            traceIndexBuckets.add(new TraceIndexBucket(Constants.CLIENT_SMALL_BUCKET_MAP_SIZE));
        }
    }
}
