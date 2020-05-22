package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.*;

import static com.ayang818.middleware.tailbase.Constants.PROCESS_COUNT;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/16 19:39
 **/
@ChannelHandler.Sharable
public class MessageHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private static ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /**
     * FINISH_PROCESS_COUNT will add one, when process call finish();
     */
    private static volatile Integer FINISH_PROCESS_COUNT = 0;

    /**
     * save 90 buckets for wrong trace
     */
    private static final int BUCKET_COUNT = 90;

    private static final List<TraceIdBucket> TRACEID_BUCKET_LIST = new ArrayList<>();

    public static void init() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            TRACEID_BUCKET_LIST.add(new TraceIdBucket());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {
        String text = msg.text();
        JSONObject jsonObject = JSON.parseObject(text);
        Integer type = jsonObject.getObject("type", Integer.class);

        switch (type) {
            case Constants.UPDATE_TYPE:
                List<String> badTraceIdList = jsonObject.getObject("badTraceIdSet", new TypeReference<List<String>>() {});
                int bucketPos = jsonObject.getObject("bucketPos", Integer.class);
                setWrongTraceId(badTraceIdList, bucketPos);
                break;
            case Constants.TRACE_DETAIL:
                Map<String, List<String>> spans = jsonObject.getObject("data",
                    new TypeReference<Map<String, List<String>>>(){});
                // pull data from this pos, here is for a recent ack!
                Integer pos = jsonObject.getObject("dataPos", Integer.class);
                consumeTraceDetails(spans, pos);
                break;
            default:
                break;
        }
    }

    private void consumeTraceDetails(Map<String, List<String>> detailMap, Integer pos) {
        HashMap<String, List<String>> tmpMap = new HashMap<>(32);
        Iterator<Map.Entry<String, List<String>>> it = detailMap.entrySet().iterator();

        Map.Entry<String, List<String>> entry;
        while (it.hasNext()) {
            entry = it.next();
            // TODO
        }
    }

    public static void pullWrongTraceDetails(String traceIdListString, int bucketPos) {
        String msg = String.format("{\"type\": %d, \"traceIdList\": %s, \"bucketPos\": %d}",
                Constants.PULL_TRACE_DETAIL_TYPE, traceIdListString, bucketPos);
        channels.writeAndFlush(new TextWebSocketFrame(msg));
    }

    public void setWrongTraceId(List<String> badTraceIdList, int bucketPos) {
        int pos = bucketPos % BUCKET_COUNT;
        TraceIdBucket traceIdBucket = TRACEID_BUCKET_LIST.get(pos);
        if (traceIdBucket.getBucketPos() != 0 && traceIdBucket.getBucketPos() != bucketPos) {
            logger.warn("override a working bucket!!!");
        }

        if (badTraceIdList != null && badTraceIdList.size() > 0) {
            traceIdBucket.setBucketPos(bucketPos);
            int processCount = traceIdBucket.addProcessCount();
            traceIdBucket.getTraceIdList().addAll(badTraceIdList);
            logger.info(String.format("%d pos bucket process time is %d", pos, processCount));
        }
    }

    public static TraceIdBucket getFinishedBucket() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            int next = i + 1;
            if (next >= BUCKET_COUNT) {
                next = 0;
            }
            TraceIdBucket currentBatch = TRACEID_BUCKET_LIST.get(i);
            TraceIdBucket nextBatch = TRACEID_BUCKET_LIST.get(next);
            // when client process is finished, or then next trace batch is finished. to get checksum for wrong traces.
            // TODO 不懂
            if ((FINISH_PROCESS_COUNT >= PROCESS_COUNT && currentBatch.getBucketPos() > 0) ||
                    (nextBatch.getProcessCount() >= PROCESS_COUNT && currentBatch.getProcessCount() >= PROCESS_COUNT)) {
                // reset
                TraceIdBucket newTraceIdBatch = new TraceIdBucket();
                TRACEID_BUCKET_LIST.set(i, newTraceIdBatch);
                return currentBatch;
            }
        }
        return null;
    }

    public static boolean isFin() {
        for (TraceIdBucket traceIdBucket : TRACEID_BUCKET_LIST) {
            if (traceIdBucket.getBucketPos() != 0) {
                return false;
            }
        }
        return true;
    }

    public static long getStartTime(String span) {
        // String spans = entry.getValue()
        //         .stream()
        //         .sorted(Comparator.comparing(MessageHandler::getStartTime))
        //         .collect(Collectors.joining("\n"));
        if (span != null) {
            String[] cols = span.split("\\|");
            if (cols.length > 8) {
                return BaseUtils.toLong(cols[1], -1);
            }
        }
        return -1;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        channels.add(ctx.channel());
        logger.info("添加一条新的连接，当前总共 {} 条连接......", channels.size());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        channels.remove(ctx.channel());
        logger.info("删除一条连接，当前总共 {} 条连接......", channels.size());
    }

    public String md5(String key) {
        char[] hexDigits = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
        };
        try {
            byte[] btInput = key.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            return null;
        }
    }

    // private void check(Map<String, String> res) {
    //     String path = "D:\\middlewaredata\\checkSum.data";
    //     try {
    //         byte[] bytes = Files.readAllBytes(Paths.get(path));
    //         String standardRes = new String(bytes);
    //         Map<String, String> standardMap = JSON.parseObject(standardRes, new
    //        TypeReference<Map<String, String>>() {
    //         });
    //         int trueNum = 0;
    //         int num = 0;
    //         for (Map.Entry<String, String> entry : standardMap.entrySet()) {
    //             num++;
    //             if (res.containsKey(entry.getKey())) {
    //                 String s1 = res.get(entry.getKey());
    //                 String s2 = entry.getValue();
    //                 if (s1.equals(s2)) {
    //                     trueNum++;
    //                 } else {
    //                     if ("3accc15c0f094397".equals(entry.getKey())) {
    //                         logger.info("here!!!");
    //                     }
    //                     StringBuilder sb = new StringBuilder();
    //                     sb.append("\n----------\n");
    //                     sb.append(entry.getKey()).append("\n");
    //                     List<String> spansList = errTraceMap.get(entry.getKey());
    //                     sb.append(spansList.size()).append("\n");
    //                     spansList.forEach(str -> sb.append(str).append("\n"));
    //                     Files.write(Paths.get("D:/middlewaredata/myres.data"), sb.toString()
    //                    .getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    //                 }
    //             }
    //         }
    //         logger.info(String.format("共收集到 %d 条链路，总共有 %d 条链路正确，正确率 %f", num, trueNum, (
    //        (double) trueNum / standardMap.size()) * 100));
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }
}
