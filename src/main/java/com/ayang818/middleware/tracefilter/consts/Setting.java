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
     * 由于发送 pending 请求过于频繁，所以需要设置缓冲区数组长度
     */
    public static final int PENDING_BUFFER_SIZE = 200;

    public static boolean stdoutFlag = true;

    /**
     *  行距超过 100000 直接删除
     */
    public static final Integer REMOVE_ANYWAY_THRESHOLD = 100000;

    /**
     *  扫描 map 的间隔
     */
    public static final Integer SCAN_TRACE_MAP_INTERVAL = 20000;

    /**
     * 大部分单条数据长度（byte）
     */
    public static final Integer NORMAL_PER_LINE_SIZE = 270;

    public static final String STATUS_CODE = "http.status_code";

    public static final String PASS_STATUS_CODE = "200";

    public static final String ERROR = "error";

    public static final String BAN_ERROR_CODE = "1";

    /**
     *  【websocket frame信息标识】 ： 错误链路
     */
    public static final int ERROR_TYPE = 0;

    /**
     *  【websocket frame信息标识】 ： 等待判定链路
     */
    public static final int PENDING_TYPE = 1;

    /**
     *  【websocket frame信息标识】 ： 表示所有数据均已经发送完
     */
    public static final int FIN_TYPE = 2;
}
