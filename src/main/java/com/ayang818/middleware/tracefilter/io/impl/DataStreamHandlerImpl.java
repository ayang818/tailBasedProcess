package com.ayang818.middleware.tracefilter.io.impl;

import com.ayang818.middleware.tracefilter.io.DataStreamHandler;
import com.ayang818.middleware.tracefilter.utils.SplitterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

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
     * 换行标识符
     */
    private static final char NEXT_LINE_FLAG = '\n';

    /**
     * 大部分单条数据长度
     */
    private static final Integer NORMAL_PER_LINE_SIZE = 121;

    private static final String STATUS_CODE = "http.status_code";

    private static final String PASS_STATUS_CODE = "200";

    private static final String ERROR = "error";

    private static final String BAN_ERROR_CODE = "1";

    @Override
    public void handleDataStream(InputStream dataStream) {
        logger.info("连接数据源成功，开始拉取数据......");
        ReadableByteChannel inChannel = Channels.newChannel(dataStream);
        // set nio read Buffer
        ByteBuffer readByteBuffer = ByteBuffer.allocateDirect(defaultReadBufferSize);
        try {
            filterLine(inChannel, readByteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void filterLine(ReadableByteChannel inChannel, ByteBuffer readByteBuffer) throws IOException {
        long startTime = System.currentTimeMillis();
        byte[] bytes = new byte[defaultReadBufferSize];
        StringBuilder strbuilder = new StringBuilder();
        long sumbytes = 0;
        int lineNumber = 0;
        while (inChannel.read(readByteBuffer) != -1) {
            // 反转准备读
            readByteBuffer.flip();
            int bytesLen = readByteBuffer.remaining();
            // 读入临时bytes数组
            readByteBuffer.get(bytes, 0, bytesLen);
            readByteBuffer.clear();
            String tmpstr = new String(bytes, 0, bytesLen);
            char[] chars = tmpstr.toCharArray();
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == NEXT_LINE_FLAG) {
                    // 得到一行数据
                    String lineData = strbuilder.toString();
                    // 统计收到的字节数，这个没啥用
                    sumbytes += lineData.getBytes().length;
                    // 当前处于第 count 条数据，一条traceId中的窗口大约为 2w 条数据
                    lineNumber += 1;
                    // TODO : how to handle each line data
                    handleLine(lineData, lineNumber);
                    strbuilder.delete(0, strbuilder.length());
                } else {
                    strbuilder.append(chars[i]);
                }
            }
        }
        logger.info("拉取数据源完毕，耗时 {} ms......", System.currentTimeMillis() - startTime);
        logger.info("共拉到 {} 行数据，每行数据平均大小 {} 字节", lineNumber, sumbytes / lineNumber);
    }

    @Override
    public void handleLine(String line, Integer lineNumber) {
        // 每行数据总共9列，以8个 | 号做分割
        if (line == null) {
            logger.info("此行为空");
            return ;
        }
        String[] data = SplitterUtil.baseSplit(line, "\\|");
        String traceId = data[0];
        String tags = data[8];
        String[] eachTag = SplitterUtil.baseSplit(tags, "&");
        boolean isWrong = false;
        // 遍历所有 tag，找到是否有符合要求的 tag，如果有则将这条有问题的 span 所在的 traceId 上报给通信中心
        for (int i = 0; i < eachTag.length; i++) {
            String[] kvArray = SplitterUtil.baseSplit(eachTag[i], ":");
            if (kvArray != null && kvArray.length == 2) {
                String key = kvArray[0];
                String val = kvArray[1];
                boolean satisfyStatusCode = STATUS_CODE.equals(key) && !PASS_STATUS_CODE.equals(val);
                boolean satisfyError = ERROR.equals(key) && BAN_ERROR_CODE.equals(val);
                // 过滤条件 : http.status_code!=200 || error=1
                if (satisfyStatusCode || satisfyError) {
                    isWrong = true;
                    break;
                }
            }
        }
        if (isWrong) {
            // TODO : 发现错误惧之后的处理 ：1. 上报数据中心。 2. 窗口期向后延长 500(暂定)。 3. 本地落盘数据删除。
            reportTraceId(traceId);
        }
    }

    private void reportTraceId(String traceId) {

    }


}
