package com.ayang818.middleware.tailbase.utils;

import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;

import java.io.*;

import static com.ayang818.middleware.tailbase.Constants.standardBytes;

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

            // if (!contains(bts,
            //         0, len, standardBytes[2])) {
            //     if (contains(bts,
            //             0, len, standardBytes[1]) || contains(bts, 0, len, standardBytes[0])) {
            //         isWrongSpan = true;
            //     }
            // }

            // 这个找出的更多。。。。
            // isWrongSpan = (contains(bts, 0, len, standardBytes[0]) || contains(bts,
            //         0, len, standardBytes[1]) && !contains(bts,
            //         0, len, standardBytes[2]));

            // 713
            // String tmp = new String(bts);
            // if (tmp.contains("error=1") || (tmp.contains("http.status_code") && !tmp.contains("http.status_code=200")))  wrongNum1++;

            if (tag.contains("error=1") || (tag.contains("http.status_code") && !tag.contains("http.status_code=200")))  isWrongSpan = true;

            if (isWrongSpan) wrongNum1++;
        }
        System.out.println(String.format("cost %d", System.currentTimeMillis() - start));
        System.out.println(wrongNum1);
        System.out.println(String.format("tags len avg is %d", sum / time));
    }

    public static boolean contains(byte[] bts, int s, int e, byte[] str) {
        return ClientDataStreamHandler.BlockWorker.contains(bts, s, e, str);
    }
}
