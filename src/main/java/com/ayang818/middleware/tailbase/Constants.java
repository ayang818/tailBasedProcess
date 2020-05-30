package com.ayang818.middleware.tailbase;

/**
 * @author 杨丰畅
 * @description 常量池
 * @date 2020/5/22 21:34
 **/
public class Constants {
    public static final String CLIENT_PROCESS_PORT1 = "8000";
    public static final String CLIENT_PROCESS_PORT2 = "8001";
    public static final String BACKEND_PROCESS_PORT1 = "8002";
    public static int BUCKET_SIZE = 20000;
    public static int TARGET_PROCESS_COUNT = 2;

    public static final int UPDATE_TYPE = 0;
    public static final int TRACE_DETAIL = 1;
    public static final int FIN_TYPE = 2;
    public static final int CHANNEL_TYPE = 3;

    public static final int SENDER_TYPE = 0;
    public static final int RECEIVER_TYPE = 1;

    public static final int PULL_TRACE_DETAIL_TYPE = 0;
}
