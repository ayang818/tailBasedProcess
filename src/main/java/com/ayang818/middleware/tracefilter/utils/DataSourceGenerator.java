package com.ayang818.middleware.tracefilter.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 杨丰畅
 * @description 这版本的生成器仅提供一份格式正确的数据
 * @date 2020/5/5 20:33
 **/
public class DataSourceGenerator {

    private static final String BASE_DIR = "D:/middlewaredata/";

    private static final long TARGET_FILE_SIZE = 1024 * 1024 * 100;

    private static final ExecutorService FLUSH_THREAD = Executors.newSingleThreadExecutor();

    private static final String[] STATUS_TYPE = new String[]{"http.status_code:200", "http.status_code:404", "error:1"};

    public static void main(String[] args) {
        DataSourceGenerator generator = new DataSourceGenerator();
        //generator.mmapGenerate();
        generator.nioGenerate();
        //generator.bioGenerate();
    }

    /**
     * nio + 缓冲区
     */
    public void nioGenerate() {
        String fileName = "nio_data.txt";
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        // 申请 100条 记录的堆外内存
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(192 * 100);
        // 申请 64KB 的二级缓存
        ByteBuffer flushBuffer = ByteBuffer.allocateDirect(1024 * 64);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(BASE_DIR + fileName, "rw");
             FileChannel channel = randomAccessFile.getChannel()) {
            while (true) {
                byte[] bytesLine = generateLine().getBytes();
                if (channel.size() + bytesLine.length >= TARGET_FILE_SIZE) break;
                int remain = byteBuffer.remaining();
                if (bytesLine.length > remain) {
                    flush(byteBuffer, flushBuffer, channel);
                }
                byteBuffer.put(bytesLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long delta = System.currentTimeMillis() - start;
        System.out.println("[缓冲区nio] : 数据生成完成");
        System.out.println("花费 " + delta + " 毫秒，速度为 " + String.format("%.2f", ((double) TARGET_FILE_SIZE / (1024 * 1024)) / (delta / 1000)) + " MB/S");
    }


    /**
     * @param byteBuffer 一级缓存
     * @param flushBuffer 二级刷盘缓存
     * @param channel 管道
     * @description 缓冲区容量不足，写入二级缓存，若二级缓存即将慢，进行刷盘
     */
    public void flush(ByteBuffer byteBuffer, ByteBuffer flushBuffer, FileChannel channel) {
        // 反转一级缓存为读Mode
        byteBuffer.flip();
        // 剩余可读缓存
        int remain = byteBuffer.remaining();
        byte[] records = new byte[remain];
        byteBuffer.get(records);
        byteBuffer.clear();
        // 其他线程异步刷盘
        FLUSH_THREAD.execute(() -> {
            try {
                // 检查二级缓存是否足够
                if (flushBuffer.remaining() < records.length) {
                    // 读出二级缓存中的内容
                    flushBuffer.flip();
                    // 写入管道
                    channel.write(flushBuffer);
                    flushBuffer.clear();
                }
                flushBuffer.put(records);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 使用内存映射技术进行文件写入
     */
    public void mmapGenerate() {
        String fileName = "mmap_data.txt";
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        try (RandomAccessFile memoryAccessFile = new RandomAccessFile(BASE_DIR + fileName, "rw")) {
            FileChannel fileChannel = memoryAccessFile.getChannel();
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TARGET_FILE_SIZE);
            // 设置内存初始偏移段
            int offset = 0;
            while (true) {
                byte[] bytes = generateLine().getBytes();
                // 计算内存偏移段+即将加入的字节数量是否超出一开始的限制
                if (offset + bytes.length > TARGET_FILE_SIZE) break;
                // 设置映射起始位置
                mappedByteBuffer.position(offset);
                mappedByteBuffer.put(bytes);
                // 移动偏移段
                offset += bytes.length;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long delta = System.currentTimeMillis() - start;
        System.out.println("[内存映射] : 数据生成完成");
        System.out.println("花费 " + delta + " 毫秒，速度为 " + String.format("%.2f", ((double) TARGET_FILE_SIZE / (1024 * 1024)) / (delta / 1000)) + " MB/S");
    }

    /**
     * 带缓冲区的bio，目前看来这是表现最好的方式了
     */
    public void bioGenerate() {
        String fileName = "bio_data.txt";
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        File dataFile = new File(BASE_DIR + fileName);
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        while (!dataFile.exists() || dataFile.length() < TARGET_FILE_SIZE) {
            // 若文件不存在，则创建
            if (!dataFile.exists()) {
                try {
                    dataFile.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // 生成一行数据
            String lineString = generateLine();
            try {
                bufferedWriter.write(lineString);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        long delta = System.currentTimeMillis() - start;
        System.out.println("[缓冲区bio] : 数据生成完成");
        System.out.println("花费 " + delta + " 毫秒，速度为 " + String.format("%.2f", ((double) TARGET_FILE_SIZE / (1024 * 1024)) / (delta / 1000)) + " MB/S");
    }

    /**
     * @return
     * @description traceId(全局唯一Id)|startTime(调用开始的时间)|spanId(调用链环节某条数据Id)|parentSpanId(某条数据的父节点Id)
     * |duration(调用耗时)|serviceName(调用的服务名)|spanName(调用的埋点名)|host(机器标识，比如ip，机器名)|tags|
     * 共 9 种
     */
    public String generateLine() {
        StringBuilder res = new StringBuilder();
        UUID uuid = UUID.randomUUID();

        res.append(uuid.toString()).append("|");
        res.append(System.currentTimeMillis()).append("|");
        res.append(UUID.randomUUID().toString()).append("|");
        res.append(UUID.randomUUID().toString()).append("|");
        res.append(((int) (Math.random() * 100000)) + 200).append("|");
        res.append("randomService").append("|");
        res.append("randomSpanName").append("|");
        res.append("127.0.0.1").append("|");

        int types = (int) ((Math.random() * 100));
        // 5%的概率失败
        if (types < 5) {
            res.append(STATUS_TYPE[1]).append("&").append(STATUS_TYPE[2]);
        } else {
            res.append(STATUS_TYPE[0]);
        }
        res.append("\n");

        return res.toString();
    }

}
