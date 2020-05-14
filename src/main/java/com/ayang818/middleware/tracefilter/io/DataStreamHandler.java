package com.ayang818.middleware.tracefilter.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:53
 **/
public interface DataStreamHandler {


    /**
     * @param dataStream 从输入容器读到的数据流
     * @description 对于输入容器中的输入流做处理
     */
    void handleDataStream(InputStream dataStream);

    /**
     * @param inChannel      读入管道
     * @param readByteBuffer 读入缓存
     * @description 将数据分解成行为单位
     */
    void filterLine(ReadableByteChannel inChannel, ByteBuffer readByteBuffer) throws IOException;


    /**
     * @description 处理每行数据
     * @param line 单行数据
     * @param count
     * @return
     */
    boolean handleLine(String line, Integer count);
}
