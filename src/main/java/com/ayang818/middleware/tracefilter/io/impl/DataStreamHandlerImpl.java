package com.ayang818.middleware.tracefilter.io.impl;

import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:47
 **/
@Service
public class DataStreamHandlerImpl implements DataStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(DataStreamHandlerImpl.class);

    /**
     * 默认读缓冲区大小，由于系统默认的用户地址空间文件缓冲区大小4096字节，所以这里取4096的倍数4MB
     */
    private final Integer defaultReadBufferSize = 1024 * 1024 * 4;

    /**
     * @description 对于输入容器中的输入流做处理
     * @param dataStream
     */
    @Override
    public void handleDataStream(InputStream dataStream) {
        InputStreamReader inputStreamReader = new InputStreamReader(dataStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader, 1024 * 1024 * 4);
        try {
            String lineData;
            while ((lineData = bufferedReader.readLine()) != null) {
                System.out.println(Arrays.toString(lineData.toCharArray()));
                System.out.println(lineData.toCharArray().length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void flush(byte[] dataline) {

    }
}
