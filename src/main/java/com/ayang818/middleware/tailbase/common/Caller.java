package com.ayang818.middleware.tailbase.common;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.List;
import java.util.Set;

/**
 * <p>caller</p>
 *
 * @author : chengyi
 * @date : 2020-06-16 20:41
 **/
public class Caller {
    @JSONField(name = "type")
    public int type;
    @JSONField(name = "data")
    public List<PullDataBucket> data;

    public Caller(int type, List<PullDataBucket> data) {
        this.type = type;
        this.data = data;
    }

    public int getType() {
        return type;
    }

    public List<PullDataBucket> getData() {
        return data;
    }

    /**
     * 一个需要被消费的bucket
     */
    public static class PullDataBucket {
        @JSONField(name = "errTraceIdSet")
        public Set<String> errTraceIdSet;
        @JSONField(name = "pos")
        public Integer pos;

        public PullDataBucket(Set<String> errTraceIdSet, Integer pos) {
            this.errTraceIdSet = errTraceIdSet;
            this.pos = pos;
        }

        public Set<String> getErrTraceIdSet() {
            return errTraceIdSet;
        }

        public int getPos() {
            return pos;
        }

        public void setErrTraceIdSet(Set<String> errTraceIdSet) {
            this.errTraceIdSet = errTraceIdSet;
        }

        public void setPos(int pos) {
            this.pos = pos;
        }
    }
}
