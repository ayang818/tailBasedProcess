package com.ayang818.middleware.tailbase.utils;

import com.google.gson.Gson;

/**
 * @author 杨丰畅
 * @description Gson工具类 用来替代fastjson
 * @date 2020/5/29 0:19
 **/
public class GsonUtils {
    private static Gson gson = new Gson();

    public static Gson getGson() {
        return gson;
    }
}
