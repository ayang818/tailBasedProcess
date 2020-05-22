package com.ayang818.middleware.tailbase.utils;

import com.ayang818.middleware.tailbase.Constants;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/22 21:33
 **/
public class BaseUtils {

    private final static OkHttpClient OK_HTTP_CLIENT = new OkHttpClient.Builder()
            .connectTimeout(50L, TimeUnit.SECONDS)
            .readTimeout(60L, TimeUnit.SECONDS)
            .build();

    public static Response callHttp(Request request) throws IOException {
        Call call = OK_HTTP_CLIENT.newCall(request);
        return call.execute();
    }

    public static boolean isClientProcess() {
        String port = System.getProperty("server.port", "8080");
        if (Constants.CLIENT_PROCESS_PORT1.equals(port) ||
                Constants.CLIENT_PROCESS_PORT2.equals(port)) {
            return true;
        }
        return false;
    }

    public static boolean isBackendProcess() {
        String port = System.getProperty("server.port", "8080");
        if (Constants.BACKEND_PROCESS_PORT1.equals(port)) {
            return true;
        }
        return false;
    }

    public static long toLong(String str, long defaultValue) {
        if (str == null) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException var4) {
                return defaultValue;
            }
        }
    }
}
