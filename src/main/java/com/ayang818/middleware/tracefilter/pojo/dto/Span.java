package com.ayang818.middleware.tracefilter.pojo.dto;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/19 23:28
 **/
public class Span {
    private Long startTime;
    private String lineData;

    public Span(Long startTime, String lineData) {
        this.startTime = startTime;
        this.lineData = lineData;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public String getLineData() {
        return lineData;
    }

    public void setLineData(String lineData) {
        this.lineData = lineData;
    }

    @Override
    public String toString() {
        return String.format("{\"startTime\": %d, \"lineData\": \"%s\"}", this.startTime, this.lineData);
    }
}
