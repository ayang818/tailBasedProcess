package com.ayang818.middleware.tailbase.client;

import com.ayang818.middleware.tailbase.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * 大桶，存储一个小桶列表
 */
public class BigBucket {
    List<SmallBucket> smallBuckets;

    public BigBucket() {
        this.smallBuckets = new ArrayList<>(Constants.CLIENT_SMALL_BUCKET_COUNT);
    }

    public SmallBucket getSmallBucket(int innerPos) {
        if (innerPos < 0 || innerPos > Constants.CLIENT_SMALL_BUCKET_COUNT) {
            throw new IndexOutOfBoundsException(String.format("innerPos %d out of bigBucket", innerPos));
        }
        return smallBuckets.get(innerPos);
    }

    public List<SmallBucket> getSmallBucketList() {
        return smallBuckets;
    }

    public void init() {
        for (int i = 0; i < Constants.CLIENT_SMALL_BUCKET_COUNT; i++) {
            smallBuckets.add(new SmallBucket(Constants.CLIENT_SMALL_BUCKET_MAP_SIZE));
        }
    }
}
