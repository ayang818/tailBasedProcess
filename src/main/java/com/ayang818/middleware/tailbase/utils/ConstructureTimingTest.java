package com.ayang818.middleware.tailbase.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/6/13 0:41
 **/
public class ConstructureTimingTest {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws IOException {
        ConstructureTimingTest c = new ConstructureTimingTest();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 64);
        ReadableByteChannel channel = Channels.newChannel(new FileInputStream("D" +
                ":\\middlewaredata\\trace1.data"));
        long startTime = System.currentTimeMillis();
        System.out.println("start it");
        while (channel.read(byteBuffer) != -1) {
            byteBuffer.flip();
            int remain = byteBuffer.remaining();
            byte[] bytes = new byte[remain];
            byteBuffer.get(bytes, 0, remain);
            byteBuffer.clear();
            // 单构造函数预计要快 有ture的会慢一点
            c.executorService.execute(new Worker(bytes, true));
        }
        c.executorService.execute(() -> {
            long endTime = System.currentTimeMillis();
            System.out.println(String.format("cost time %ds", endTime - startTime));
        });
    }

    private static class Worker implements Runnable {
        private byte[] bytes;
        private char[] chars;
        private boolean direct = false;
        String data;
        public Worker(byte[] bytes) {
            this.bytes = bytes;
        }

        public Worker(byte[] bytes, boolean direct) {
            this.data = new String(bytes);
            this.chars = this.data.toCharArray();
            this.direct = direct;
        }

        @Override
        public void run() {
            if (!direct) {
                this.data = new String(bytes);
                chars = this.data.toCharArray();
            }
        }
    }
}
