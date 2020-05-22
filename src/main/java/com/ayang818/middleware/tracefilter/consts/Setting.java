package com.ayang818.middleware.tracefilter.consts;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/17 17:22
 **/
public class Setting {

    /**
     * 默认读缓冲区大小，由于系统默认的用户地址空间文件缓冲区大小4096字节，所以这里取4096的倍数4MB
     */
    public static final Integer defaultReadBufferSize = 1024 * 1024 * 4;

    /**
     *  扫描 map 的间隔
     */
    public static final Integer SEND_TRACE_MAP_INTERVAL = 20000;

    public static final Integer SEND_THRESHOLD = 40000;

    /**
     *  【websocket frame信息标识】 ： 错误链路
     */
    public static final int ERROR_TYPE = 0;

    /**
     *  【websocket frame信息标识】 ： 等待判定链路
     */
    public static final int REAL_FIN_TYPE = 3;

    /**
     *  【websocket frame信息标识】 ： 表示所有数据均已经发送完
     */
    public static final int FIN_TYPE = 2;

    public static final String PULL_ERR_MSG = "{\"type\": 1}";

}
