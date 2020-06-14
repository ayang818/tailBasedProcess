package com.ayang818.middleware.tailbase.utils;

import java.util.Arrays;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-06-14 14:31
 **/
public class SpeedTest {
    public static void main(String[] args) {
        String spl = "|";
        System.out.println(Arrays.toString(spl.getBytes()));
        String lf = "\n";
        System.out.println(Arrays.toString(lf.getBytes()));
        String err1 = "error=1";
        System.out.println(Arrays.toString(err1.getBytes()));
        String err2 = "http.status_code=200";
        String str1 = "200";
        String str2 = "http.status_code=";
        System.out.println(Arrays.toString(err2.getBytes()));
        System.out.println(Arrays.toString(str1.getBytes()));
        System.out.println(Arrays.toString(str2.getBytes()));

        // new String 快 还是 ArrayCopy快
        // String data = "d614959183521b4b|1587457763183000|dffcd4177c315535|d614959183521b4b|980|Loginc|getLogisticy|192.168.1.2|http.status_code=200";
        // byte[] bytes = data.getBytes();
        // long start = System.currentTimeMillis();
        // for (int i = 0; i < 1000000; i++) {
        //     new String(bytes);
        // }
        // long end = System.currentTimeMillis();
        // System.out.println(end - start);
        //
        // start = System.currentTimeMillis();
        // for (int i = 0; i < 1000000; i++) {
        //     byte[] target = new byte[bytes.length];
        //     System.arraycopy(bytes, 0, target, 0, bytes.length);
        // }
        // end = System.currentTimeMillis();
        // System.out.println(end - start);
    }
}
