package com.ayang818.middleware.tracefilter.utils;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 20:18
 **/
public class CastUtil {
    public static byte[] chars2bytes(char[] chars, byte[] bytes) {
        for (int i = 0; i < chars.length; i++) {
            bytes[i] = (byte) chars[i];
        }
        return bytes;
    }
}
