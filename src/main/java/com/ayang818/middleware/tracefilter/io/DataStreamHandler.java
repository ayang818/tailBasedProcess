package com.ayang818.middleware.tracefilter.io;

import java.io.InputStream;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/5 12:53
 **/
public interface DataStreamHandler {
    void handleDataStream(InputStream dataStream);
}
