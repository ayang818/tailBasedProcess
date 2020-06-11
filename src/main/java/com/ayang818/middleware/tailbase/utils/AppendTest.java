package com.ayang818.middleware.tailbase.utils;
import java.util.Arrays;

/**
 * <p>append test</p>
 *
 * @author : chengyi
 * @date : 2020-06-11 13:46
 **/
public class AppendTest {

    /**
     * Main
     */
    public static void main(String[] args) {
        char[] chars = new char[1024 * 64];
        Arrays.fill(chars, 'a');
        StringBuilder sb = new StringBuilder();
        int target = 1024 * 64;
        int l = 0;

        long start = System.currentTimeMillis();
        while (l < target) {
            for (int i = 0; i < chars.length; i++) {
                if (i % 70 == 0) {
                    sb.delete(0, sb.length());
                }
                sb.append(chars[i]);
            }
            l += 1;
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("cost time %d ms", end - start));

        l = 0;
        start = System.currentTimeMillis();
        while (l < target) {
            for (int i = 0; i < chars.length; i++) {
                if (i % 70 == 0 && i != 0) {
                    sb.append(chars, i - 70, 70);
                    sb.delete(0, sb.length());
                }
            }
            l += 1;
        }
        end = System.currentTimeMillis();

        System.out.println(String.format("continuous append cost time %d ms", end - start));
    }
}
