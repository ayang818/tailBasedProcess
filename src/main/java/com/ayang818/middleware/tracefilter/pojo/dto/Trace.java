package com.ayang818.middleware.tracefilter.pojo.dto;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 杨丰畅
 * @description 表示一条trace,其中 ：
 * spans 暂存 2w 行窗口内的数据，会有一个线程每隔2w条数据左右就来扫描一次所有的Trace，
 * 要是有Trace的lastOccurrenceLine和currentLine之间的差值达到2w以上，会向数据汇总中心询问其他处理程序这条链路有没有问题，
 * 如果得到没有问题的回复，删除此对象，释放内存。
 * @date 2020/5/14 16:36
 **/
public class Trace {
    /**
     * traceId
     */
    private String traceId;

    /**
     * 上一次出现行号
     */
    private Integer firstOccurrenceLine;

    /**
     * 是否为正确链路
     */
    private Boolean isNormalTrace;

    /**
     * 这条trace中的每条span
     */
    private List<Span> spans;

    public Trace(String traceId, Integer lineNumber) {
        this.traceId = traceId;
        this.firstOccurrenceLine = lineNumber;
        this.isNormalTrace = true;
        // 经过代码统计，一条trace中的平均值为17
        this.spans = new ArrayList<>(20);
    }

    /**
     * @description 添加新的span
     * @param lineData 一行数据
     * @param startTime
     */
    public void addNewSpan(String lineData, String startTime) {
        this.spans.add(new Span(Long.valueOf(startTime), lineData));
    }

    /**
     * @description 设置为错误的链路
     */
    public void setAsErrorTrace() {
        this.isNormalTrace = false;
    }

    public String getTraceId() {
        return this.traceId;
    }

    public Integer getFirstOccurrenceLine() {
        return this.firstOccurrenceLine;
    }

    public boolean isNormalTrace() {
        return this.isNormalTrace;
    }

    public List<Span> getSpans() {
        return this.spans;
    }
}