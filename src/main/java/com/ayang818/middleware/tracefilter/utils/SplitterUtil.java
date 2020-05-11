package com.ayang818.middleware.tracefilter.utils;

import com.google.common.base.Splitter;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/7 19:02
 **/
public class SplitterUtil {

    public static void main(String[] args) {
        String tmp = "http.status_code:200&error:1";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i<=10000; i++) {
            sb.append(tmp);
            if (i < 10000) {
                sb.append("&");
            }
        }

        //long start1 = System.currentTimeMillis();
        //String[] res1 = baseSplit(sb.toString(), "&");
        //System.out.printf("spend %d\n", System.currentTimeMillis() - start1);
        //System.out.println(res1.length);
        //System.out.println(Arrays.toString(res1));
        //long start2 = System.currentTimeMillis();
        //Iterable<String> stringIterable = extendSplit(sb.toString(), "&");
        //System.out.printf("spend %d\n", System.currentTimeMillis() - start2);
        //AtomicInteger i = new AtomicInteger();
        //String[] res2 = new String[res1.length+1];
        //stringIterable.forEach((str) -> {res2[i.get()] = str; i.getAndIncrement();});
        //System.out.println(Arrays.toString(res2));
        baseSplit(sb.toString(), "&");
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
