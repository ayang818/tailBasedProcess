package com.ayang818.middleware.tracefilter.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author 杨丰畅
 * @description 这版本的生成器仅提供一份格式正确的数据
 * @date 2020/5/5 20:33
 **/
public class DataSourceGenerator {

    private static final String PATH = "D:/middlewaredata/data.txt";

    private static final long TARGET_FILE_SIZE = 1024 * 1024 * 100;

    private static final Executor SINGLE_FLUSH_THREAD = Executors.newSingleThreadExecutor();

    private static final String[] STATUS_TYPE = new String[]{"http.status_code:200", "http.status_code:404", "error:1"};

    public static void main(String[] args) {
        DataSourceGenerator generator = new DataSourceGenerator();
        generator.generate();
        generator.bioGenerate();
    }

    public void generate() {
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        Set<String> idSet = new HashSet<>();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024 * 4);

        File dataFile = new File(PATH);
        FileOutputStream fileOutputStream = null;
        FileChannel outputChannel = null;

        try {
            fileOutputStream = new FileOutputStream(dataFile);
            outputChannel = fileOutputStream.getChannel();
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
            String lineString = generateLine(idSet);
            byte[] lineBytes = lineString.getBytes();
            // 判断缓冲区是否足够
            if (byteBuffer.remaining() < lineBytes.length) {
                // 缓存区刷盘
                flush(byteBuffer, outputChannel);
            }
            // 缓存区写入数据
            byteBuffer.put(lineBytes);
        }

        long delta = System.currentTimeMillis() - start;
        System.out.println("[缓冲区nio] : 数据生成完成");
        System.out.println("花费 " + delta + " 毫秒，速度为 " + String.format("%.2f", ((double) TARGET_FILE_SIZE / (1024 * 1024)) / (delta / 1000)) + " MB/S");
    }


    /**
     * @param byteBuffer
     * @param outputChannel
     * @description 缓冲区容量不足，进行刷盘
     */
    public void flush(ByteBuffer byteBuffer, FileChannel outputChannel) {
        // 反转准备读
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            try {
                outputChannel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byteBuffer.clear();
    }

    /**
     * @param idSet
     * @return
     * @description traceId(全局唯一Id)|startTime(调用开始的时间)|spanId(调用链环节某条数据Id)|parentSpanId(某条数据的父节点Id)
     * |duration(调用耗时)|serviceName(调用的服务名)|spanName(调用的埋点名)|host(机器标识，比如ip，机器名)|tags|
     * 共 9 种
     */
    public String generateLine(Set<String> idSet) {
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

    /**
     * 带缓冲区的bio
     */
    public void bioGenerate() {
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        String suffix = "_bio";
        File dataFile = new File(PATH + suffix);
        Set<String> idSet = new HashSet<>();
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
            String lineString = generateLine(idSet);
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
     * 没有 Buffer 的 nio 写入速度对比，这个速度是最慢的
     */
    public void noBufferGenerate() {
        long start = System.currentTimeMillis();
        System.out.println("开始生成数据");

        String suffix = "_nobuffer";
        File dataFile = new File(PATH + suffix);
        Set<String> idSet = new HashSet<>();

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
            String lineString = generateLine(idSet);
            try {
                Files.write(Paths.get(PATH + suffix), lineString.getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("[无缓冲区nio] : 数据生成完成");
        System.out.println("花费 " + (System.currentTimeMillis() - start) + " 毫秒");
    }

}
