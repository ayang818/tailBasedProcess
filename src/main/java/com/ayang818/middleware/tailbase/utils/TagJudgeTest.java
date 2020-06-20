package com.ayang818.middleware.tailbase.utils;

import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author 杨丰畅
 * @description tags匹配算法
 * @date 2020/6/20 21:46
 **/
public class TagJudgeTest {

    public static final byte[][] standardBytes = {
            {101, 114, 114, 111, 114, 61, 49},
            {104, 116, 116, 112, 46, 115, 116, 97, 116, 117, 115, 95, 99, 111, 100, 101, 61},
            {50, 48, 48},
            {104, 116, 116, 112, 46, 115, 116, 97, 116, 117, 115, 95, 99, 111, 100, 101, 61, 50,
                    48, 48}};

    public static final int[] targetPos = {standardBytes[0].length - 1, standardBytes[1].length - 1,
            standardBytes[2].length - 1};

    public static void main(String[] args) throws IOException {
        String path = "D:\\middlewaredata\\tags.data";
        long startTime = System.currentTimeMillis();
        System.out.println("start...");
        int count1 = 0;
        int count2 = 0;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String kk = "123" + line;
            byte[] bytes = kk.getBytes();
            long tmp = System.nanoTime();
            if (!normalCheck(line)) count1++;
            if (contains(bytes, 3,
                    bytes.length, standardBytes[0]) || (contains(bytes, 3, bytes.length,
                    standardBytes[1]) && !contains(bytes, 3, bytes.length, standardBytes[3]))) count2++;
            System.out.println(count1 == count2);
        }
        System.out.println(String.format("cost %dms", System.currentTimeMillis() - startTime));
        System.out.println(count1 == count2);
    }

    private static boolean contains(byte[] bts, int start, int fin, byte[] str) {
        return ClientDataStreamHandler.BlockWorker.contains(bts, start, fin, str);
    }

    public static boolean normalCheck(String tags) {
        if (tags.contains("error=1") || (tags.contains("http.status_code=") && !tags.contains("http.status_code=200"))) {
            return false;
        }
        return true;
    }

    public static boolean checkTags(String tags) {
        boolean isTrueSpan = true;
        boolean statusShown = false;
        boolean startPendingStatusCode = false;
        byte[] bytes = tags.getBytes();
        int len = bytes.length;
        int[] curPos = {-1, -1, -1};
        boolean flag = false;

        for (int i = 0; i < len; i++) {
            byte bt = bytes[i];
            for (int j = 0; j < standardBytes.length; j++) {
                if (!isTrueSpan) break;
                if (statusShown && (j == 1 || j == 2)) continue;
                if (!startPendingStatusCode && j == 2) continue;
                if (startPendingStatusCode && j == 1) continue;
                byte[] bts = standardBytes[j];
                if (bt == bts[curPos[j] + 1]) {
                    curPos[j]++;
                    if (curPos[j] == targetPos[j]) {
                        if (j == 0) {
                            isTrueSpan = false;
                        } else if (j == 1) {
                            flag = true;
                        } else {
                            statusShown = true;
                        }
                    }
                } else {
                    curPos[j] = -1;
                    if (bt == bts[curPos[j] + 1]) curPos[j]++;
                    if (j == 2) {
                        isTrueSpan = false;
                        statusShown = true;
                    }
                }
            }
            if (flag) startPendingStatusCode = true;
        }
        return isTrueSpan;
    }
}
