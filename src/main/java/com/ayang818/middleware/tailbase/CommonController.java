package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author 杨丰畅
 * @description 接收方法类
 * @date 2020/5/22 21:32
 **/
@RestController
public class CommonController {

    private static Integer DATA_SOURCE_PORT = 0;

    public static Integer getDataSourcePort() {
        return DATA_SOURCE_PORT;
    }

    @RequestMapping("/ready")
    public String ready() {
        return "suc";
    }

    @RequestMapping("/setParameter")
    public String setParameter(@RequestParam Integer port) {
        DATA_SOURCE_PORT = port;
        if (BaseUtils.isClientProcess()) {
            // start to handle data Stream
            ClientDataStreamHandler.start();
        }
        return "suc";
    }

    @RequestMapping("/start")
    public String start() {
        return "suc";
    }
}
