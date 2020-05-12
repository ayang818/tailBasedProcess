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
     * @description 对于输入容器中的输入流做处理
     * @param dataStream 从输入容器读到的数据流
     */
    void handleDataStream(InputStream dataStream);

    /**
     * @description 逐行分解数据，对于没有问题的数据
     * @param inChannel
     * @param readByteBuffer
     */
    void handleLineByLine(ReadableByteChannel inChannel, ByteBuffer readByteBuffer) throws IOException;
}
