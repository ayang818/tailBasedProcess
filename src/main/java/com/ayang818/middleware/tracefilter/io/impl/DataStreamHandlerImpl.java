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
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final Integer NORMAL_PER_LINE_SIZE = 270;

    private static final String STATUS_CODE = "http.status_code";

    private static final String PASS_STATUS_CODE = "200";

    private static final String ERROR = "error";

    private static final String BAN_ERROR_CODE = "1";

    private static final ConcurrentHashMap<String, Node> TRACEID_MAP =
            new ConcurrentHashMap<>();

    private static final HashMap<String, Integer> tmpMap = new HashMap<>();

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
        // 起始时间
        long startTime = System.currentTimeMillis();
        byte[] bytes = new byte[defaultReadBufferSize];
        StringBuilder strbuilder = new StringBuilder();
        // 总字节数，用于统计数据是否有坑 + 为后续多线程拉取速度对比提供分段标准
        long sumbytes = 0;
        // 行号
        int lineNumber = 0;
        // 错误行数
        int wrongLineNumber = 0;

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
                    // 行号
                    lineNumber += 1;
                    // 得到一行数据
                    String lineData = strbuilder.toString();
                    // 统计收到的字节数，这个没啥用
                    sumbytes += lineData.getBytes().length;
                    // TODO 处理数据 返回是否为错误行
                    wrongLineNumber += handleLine(lineData, lineNumber) ? 1 : 0;
                    strbuilder.delete(0, strbuilder.length());
                } else {
                    strbuilder.append(chars[i]);
                }
            }
        }
        logger.info("共有 {} 行数据出错", wrongLineNumber);
        logger.info("拉取数据源完毕，耗时 {} ms......", System.currentTimeMillis() - startTime);
        logger.info("共拉到 {} 行数据，每行数据平均大小 {} 字节", lineNumber, sumbytes / lineNumber);
    }

    @Override
    public boolean handleLine(String line, Integer lineNumber) {
        if (line == null) {
            logger.info("此行为空, 行号 {}", lineNumber);
            return false;
        }
        // 每行数据总共9列，以8个 | 号做分割
        String[] data = SplitterUtil.baseSplit(line, "\\|");
        // 去除掉可能导致程序报错的数据，这一部分数据过滤掉应该不会对我的代码造成太大的影响
        if (data.length < 9) {
            logger.info("不符合格式的数据 : {}, 行号 {}", line, lineNumber);
            return false;
        }

        String traceId = data[0];
        String tags = data[8];

        boolean isWrong = false;

        // 遍历所有 tag，查看这条span是否符合过滤要求
        String[] eachTag = SplitterUtil.baseSplit(tags, "&");

        for (int i = 0; i < eachTag.length; i++) {
            String[] kvArray = SplitterUtil.baseSplit(eachTag[i], "=");
            if (kvArray != null && kvArray.length == 2) {
                String key = kvArray[0];
                String val = kvArray[1];
                boolean satisfyStatusCode =
                        STATUS_CODE.equals(key) && !PASS_STATUS_CODE.equals(val);
                boolean satisfyError = ERROR.equals(key) && BAN_ERROR_CODE.equals(val);
                // 过滤条件 : http.status_code!=200 || error=1
                if (satisfyStatusCode || satisfyError) {
                    isWrong = true;
                    break;
                }
            }
        }

        if (isWrong) {
            //System.out.printf("发现错误数据 %s, 行号 %d\n", line, lineNumber);
            // TODO : 发现错误数据之后的处理 ：1. 上报数据中心。 2. 窗口期向后延长 1000(暂定)。

            // 这里由于已经知道了行号和数据了，这里可以尝试使用线程池来加速，防止IO阻塞时间过长导致效率下降
            handleWrongLine(traceId, line, lineNumber);
            return true;
        }
        return false;
    }

    private void handleWrongLine(String traceId, String lineData, Integer lineNumber) {
        // TRACEID_MAP.computeIfPresent(traceId, );
    }


}

class Node {
    public AtomicInteger showNum;
}
