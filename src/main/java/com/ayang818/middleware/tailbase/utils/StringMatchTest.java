package com.ayang818.middleware.tailbase.utils;

import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static com.ayang818.middleware.tailbase.Constants.standardBytes;
import static com.ayang818.middleware.tailbase.Constants.standardString;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-06-23 00:18
 **/
public class StringMatchTest {
    public static void main(String[] args) throws IOException {
        System.out.println("origin first");
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader("D:\\middlewaredata\\tags.data"));
        String tag;
        byte[] bts;
        long sum = 0;
        long time = 0;
        int wrongNum1 = 0;
        while ((tag = reader.readLine()) != null) {
            bts = tag.getBytes();
            boolean isWrongSpan = false;
            int len = bts.length;
            sum += len;
            time++;

            // 1. 优化后的字节数组暴力匹配
            // isWrongSpan = !contains(bts, 0, len, standardBytes[2]) && (contains(bts,
            //         0, len, standardBytes[1]) || contains(bts, 0, len, standardBytes[0]));

            // 2. 字节数组暴力匹配
            // isWrongSpan = (contains(bts, 0, len, standardBytes[0]) || contains(bts,
            //         0, len, standardBytes[1]) && !contains(bts,
            //         0, len, standardBytes[2]));

            // 3. 字符串暴力匹配
            // String tmp = new String(bts);
            // if (tmp.contains("error=1") || (tmp.contains("http.status_code") && !tmp.contains("http.status_code=200")))  wrongNum1++;
            // if (tag.contains("error=1") || (tag.contains("http.status_code") && !tag.contains("http.status_code=200")))  isWrongSpan = true;

            // 4. 字符串boyer-moore匹配
            // if (contains(tag, standardString[0]) || (contains(tag, standardString[1]) && !contains(tag, standardString[2]))) {
            //     isWrongSpan = true;
            // }

            // 5. 字节数组boyer-moore匹配
            // isWrongSpan = (contains(bts, 0, len, standardBytes[0]) || contains(bts,
            //                 0, len, standardBytes[1]) && !contains(bts,
            //                 0, len, standardBytes[2]));

            // 6. 字节数组优化boyer-moore匹配
            isWrongSpan = !contains(bts, 0, len, standardBytes[2]) && (contains(bts,
                    0, len, standardBytes[1]) || contains(bts, 0, len, standardBytes[0]));

            if (isWrongSpan) wrongNum1++;
        }
        System.out.println(String.format("cost %d", System.currentTimeMillis() - start));
        System.out.println(wrongNum1);
        System.out.println(String.format("tags len avg is %d", sum / time));
    }

    public static boolean contains(byte[] bts, int s, int e, byte[] str) {
        // return ClientDataStreamHandler.BlockWorker.contains(bts, s, e, str);
        return ClientDataStreamHandler.BlockWorker.boyerMoore(bts, s, e, str) != -1;
    }

    public static boolean contains(String origin, String pattern) {
        return boyerMoore(origin, pattern) != -1;
    }

    public static int boyerMoore(String T, String P) {
        int i = P.length() - 1;
        int j = P.length() - 1;
        do {
            if (P.charAt(j) == T.charAt(i)) {
                if (j == 0) {
                    return i; // a match!
                } else {
                    i--;
                    j--;
                }
            } else {
                i = i + P.length() - Math.min(j, 1 + last(T.charAt(i), P));
                j = P.length() - 1;
            }
        } while (i <= T.length() - 1);

        return -1;
    }

    //----------------------------------------------------------------
    // Returns index of last occurrence of character in pattern.
    //----------------------------------------------------------------
    public static int last(char c, String P) {
        for (int i = P.length() - 1; i >= 0; i--) {
            if (P.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }
}
