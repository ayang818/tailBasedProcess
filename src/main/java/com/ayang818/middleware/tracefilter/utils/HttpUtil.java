package com.ayang818.middleware.tracefilter.utils;

import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.*;
/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 10:56
 **/
public class HttpUtil {

    public static AsyncHttpClient getHttpClient() {
        return asyncHttpClient();
    }
}
