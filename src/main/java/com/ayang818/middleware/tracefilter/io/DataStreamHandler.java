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
     * @description 使用bio对于输入容器中的输入流做处理
     * @param dataStream 从输入容器读到的数据流
     */
    void handleDataStream(InputStream dataStream);

    /**
     * @description 处理每行数据
     * @param line 单行数据
     * @param count
     * @return
     */
    boolean handleLine(String line, Integer count);

}
