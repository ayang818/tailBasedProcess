package com.ayang818.middleware.tailbase.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-06-16 20:48
 **/
public class Test {
    public static void main(String[] args) throws IOException {
        System.out.println("start...");
        long startTime = System.currentTimeMillis();
        long tm = 0;
        long cost = 0;
        String path = "C:\\Users\\10042\\data\\trace1.data";
        FileInputStream fileInputStream = new FileInputStream(new File(path));
        FileChannel channel = fileInputStream.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 256);
        byte[] bts;
        int tmp = 0;
        // 遍历4g数据只需要67ms...
        // 遍历时加了if之后需要5s
        while (channel.read(byteBuffer) != -1) {
            byteBuffer.flip();
            int remain = byteBuffer.remaining();
            bts = new byte[remain];
            byteBuffer.get(bts, 0, remain);
            byteBuffer.clear();
            tm += 1;
            long l = System.nanoTime();
            for (int i = 0; i < remain; i++) {
                if (i == 10) {
                    tmp++;
                }
                if (i == 12) {
                    tmp++;
                }
            }
            long l1 = System.nanoTime();
            System.out.println(l1 - l);
            cost += (l1 - l);
        }
        System.out.println(String.format("tm: %d, 共 cost: %dms， 遍历 cost %fms", tm, System.currentTimeMillis() - startTime, (double) cost / 1000000));
    }
}
