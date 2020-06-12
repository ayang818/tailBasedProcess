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
    public static final int BACKEND_WEBSOCKET_PORT = 8003;
    // bucket间隔2w行
    public static final int BUCKET_SIZE = 20000;
    // client下bucket的数量
    public static final int CLIENT_BUCKET_COUNT = 30;
    // backend下bucket的数量
    public static final int BACKEND_BUCKET_COUNT = 100;
    // 保守估计每个traceId下只有2条数据
    public static final int CLIENT_BUCKET_MAP_SIZE = 10000;
    public static final int BUCKET_ERR_TRACE_COUNT = 20;
    // 到达次数，取决于client数量
    public static final int TARGET_PROCESS_COUNT = 2;
    // 512KB 256KB 128KB 64KB 32KB 16KB 8KB 4KB 慢慢调
    public static final int INPUT_BUFFER_SIZE = 1024 * 8;

    public static final int UPDATE_TYPE = 0;
    public static final int TRACE_DETAIL = 1;
    public static final int FIN_TYPE = 2;
    public static final int PULL_TRACE_DETAIL_TYPE = 0;
}
