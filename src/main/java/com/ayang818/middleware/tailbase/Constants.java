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

    // 每间隔2w行上报backend
    public static final int UPDATE_INTERVAL = 20000;
    // client下bigBucket的数量
    public static final int CLIENT_BIG_BUCKET_COUNT = 50;
    // client下一个bigBucket中smallBucket的数量
    public static final int CLIENT_SMALL_BUCKET_COUNT = 20;
    // 每读这么多行，切换小桶
    public static final int SWITCH_SMALL_BUCKET_INTERVAL = UPDATE_INTERVAL / CLIENT_SMALL_BUCKET_COUNT;
    // 保守估计每个traceId下只有2条数据
    public static final int CLIENT_SMALL_BUCKET_MAP_SIZE = UPDATE_INTERVAL / CLIENT_SMALL_BUCKET_COUNT / 2;
    // backend下bucket的数量
    public static final int BACKEND_BUCKET_COUNT = 100;
    // 错误链路id记录set
    public static final int BUCKET_ERR_TRACE_COUNT = 20;
    // 访问次数，取决于client数量
    public static final int TARGET_PROCESS_COUNT = 2;
    // 512KB 256KB 128KB 64KB 32KB 16KB 8KB 4KB 慢慢调 / 256 26s
    public static final int INPUT_BUFFER_SIZE = 1024 * 512;
    // 允许处理队列中最多多少任务
    public static final int SEMAPHORE_SIZE = 20000;

    public static final int UPDATE_TYPE = 0;
    public static final int TRACE_DETAIL = 1;
    public static final int FIN_TYPE = 2;
    public static final int PULL_TRACE_DETAIL_TYPE = 0;

    public static final byte[][] standardBytes = {
            {101, 114, 114, 111, 114, 61, 49},
            {104, 116, 116, 112, 46, 115, 116, 97, 116, 117, 115, 95, 99, 111, 100, 101, 61},
            {104, 116, 116, 112, 46, 115, 116, 97, 116, 117, 115, 95, 99, 111, 100, 101, 61, 50,
                    48, 48}};

    public static final String[] standardString = {
            "error=1", "http.status_code", "http.status_code=200"
    };

    public static final String finMsg = "{\"type\": 2}";

}
