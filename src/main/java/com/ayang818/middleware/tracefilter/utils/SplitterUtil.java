package com.ayang818.middleware.tracefilter.utils;

import com.google.common.base.Splitter;

import java.util.Arrays;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/7 19:02
 **/
public class SplitterUtil {

    public static void main(String[] args) {
        String tmp1 = "d614959183521b4b|1587457762878000|52637ab771da6ae6|d614959183521b4b|304284|Loginc|getAddress|192.168.1.2|http.status_code:200";
        String[] res = tmp1.split("\\|");
        System.out.println(Arrays.toString(res));
    }

    /**
     * 阅读源码得到 string.split 在单个字符下是不走正则表达式的，速度非常快
     * @param baseString
     * @param flag
     * @return
     */
    public static String[] baseSplit(String baseString, String flag) {
        if (flag == null) return new String[]{baseString};
        if (baseString == null) return null;
        // 为什么网上的文章会觉得indexOf快呢，这个不还是O(N)的操作吗
        // int i = baseString.indexOf("&");
        return baseString.split(flag);
    }

    private static Iterable<String> extendSplit(String baseString, String flag) {
        return Splitter.on(flag).split(baseString);
    }

}
