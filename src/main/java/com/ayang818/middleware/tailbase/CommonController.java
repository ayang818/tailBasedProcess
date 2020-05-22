package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.utils.BaseUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/22 21:32
 **/
public class CommonController {

    private static Integer DATA_SOURCE_PORT = 0;

    public static Integer getDataSourcePort() {
        // TODO
        return 8080;
    }

    @RequestMapping("/ready")
    public String ready() {
        return "suc";
    }

    @RequestMapping("/setParameter")
    public String setParamter(@RequestParam Integer port) {
        DATA_SOURCE_PORT = port;
        if (BaseUtils.isClientProcess()) {
            // start to handle data Stream
        }
        return "suc";
    }

    @RequestMapping("/start")
    public String start() {
        return "suc";
    }
}
