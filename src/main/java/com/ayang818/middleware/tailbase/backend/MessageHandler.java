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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ayang818.middleware.tailbase.Constants.PROCESS_COUNT;
import static com.ayang818.middleware.tailbase.backend.CheckSumService.resMap;

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

    private static final int ACK_MAP_COUNT = 45;

    private static final List<TraceIdBucket> TRACEID_BUCKET_LIST = new ArrayList<>();


    /**
     * key is pos, value is data
     */
    private static final Map<Integer, ACKData> ACK_MAP = new ConcurrentHashMap<>(100);

    private static final AtomicInteger FIN_TIME = new AtomicInteger(0);

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
                int bucketPos = jsonObject.getObject("pos", Integer.class);
                setWrongTraceId(badTraceIdList, bucketPos);
                break;
            case Constants.TRACE_DETAIL:
                Map<String, List<String>> spans = jsonObject.getObject("data",
                    new TypeReference<Map<String, List<String>>>(){});
                // pull data from this pos, here is for a recent ack!
                Integer pos = jsonObject.getObject("dataPos", Integer.class);
                consumeTraceDetails(spans, pos);
                break;
            case Constants.FIN_TYPE:
                int finTime = FIN_TIME.addAndGet(1);
                logger.info("收到 {} 次 Fin请求", finTime);
            default:
                break;
        }
    }

    /**
     * 消费从client拉到的traceId以及对应的spans
     * @param detailMap
     * @param pos
     */
    private void consumeTraceDetails(Map<String, List<String>> detailMap, Integer pos) {
        ACKData ackData = ACK_MAP.get(pos);
        if (ackData == null) {
            ackData = new ACKData();
        }

        // 剩余可访问次数
        int remainAccessTime = ackData.putAll(detailMap);

        if (remainAccessTime == 0) {

            Map<String, List<String>> map = ackData.getAckMap();
            StringBuilder sb = new StringBuilder();


            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                sb.append("traceId : ").append(entry.getKey()).append("  size : ").append(entry.getValue().size()).append("\n");

                String spans = entry.getValue()
                        .stream()
                        .sorted(Comparator.comparing(MessageHandler::getStartTime))
                        .collect(Collectors.joining("\n"));
                spans += "\n";

                sb.append(spans);
                resMap.put(entry.getKey(), md5(spans));
            }

            // 清空ackMap
            ackData.clear();

            try {
                Files.write(Paths.get("D:/middlewaredata/my.data"), sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }

            ACK_MAP.remove(pos);
            logger.info("当前检测到 {} 条错误链路", resMap.size());
            return ;
        }

        ACK_MAP.put(pos, ackData);
    }

    /**
     * 向client发送拉取数据的请求
     * @param traceIdListString
     * @param bucketPos
     */
    public static void pullWrongTraceDetails(String traceIdListString, int bucketPos) {
        String msg = String.format("{\"type\": %d, \"traceIdList\": %s, \"bucketPos\": %d}",
                Constants.PULL_TRACE_DETAIL_TYPE, traceIdListString, bucketPos);
        logger.info(msg);
        channels.writeAndFlush(new TextWebSocketFrame(msg));
        logger.info("已向client发送拉取bucket data请求.....");
    }

    /**
     * 将client主动推送过来的错误traceIdList，放置到backend 等待消费的 对应位置的 bucket 中
     * @param badTraceIdList
     * @param bucketPos
     */
    public void setWrongTraceId(List<String> badTraceIdList, int bucketPos) {
        int pos = bucketPos % BUCKET_COUNT;
        TraceIdBucket traceIdBucket = TRACEID_BUCKET_LIST.get(pos);
        if (traceIdBucket.getBucketPos() != 0 && traceIdBucket.getBucketPos() != bucketPos) {
            logger.warn("覆盖了 {} 位置的正在工作的 bucket!!!", pos);
        }
        logger.info("收到badTraceIdList : {}", badTraceIdList.toString());
        if (badTraceIdList != null && badTraceIdList.size() > 0) {
            traceIdBucket.setBucketPos(bucketPos);
            int processCount = traceIdBucket.addProcessCount();
            traceIdBucket.getTraceIdList().addAll(badTraceIdList);
            logger.info(String.format("backend %d 位置的 bucket 访问次数到达 %d", pos, processCount));
        }
    }

    /**
     * 获取一个可以被消费的bucket
     * @param startPos 上一次被消费的bucket所在的位置 + 1，既然上一个位置刚刚被消费，那么下一个位置可以被消费的概率也很大，
     *                 而且可以及时释放client端对应bucket
     * @return
     */
    public static TraceIdBucket getFinishedBucket(int startPos) {
        int end = startPos + BUCKET_COUNT;
        for (int i = startPos; i < end; i++) {
            int cur = i % BUCKET_COUNT;
            int next = (i + 1) % BUCKET_COUNT;

            TraceIdBucket currentBucket = TRACEID_BUCKET_LIST.get(cur);
            TraceIdBucket nextBucket = TRACEID_BUCKET_LIST.get(next);
            //if ((FINISH_PROCESS_COUNT >= PROCESS_COUNT && currentBatch.getBucketPos() > 0) ||
            //        (nextBatch.getProcessCount() >= PROCESS_COUNT && currentBatch.getProcessCount() >= PROCESS_COUNT)) {
            if (currentBucket.getProcessCount() >= PROCESS_COUNT) {
                // reset
                return currentBucket;
            }
        }
        return null;
    }

    /**
     * 是否结束，是否可以向评测程序发送答案
     * @return
     */
    public static boolean isFin() {
        // 是否收到对应次FIN信号
        if (FIN_TIME.get() < PROCESS_COUNT) return false;
        // bucket中元素是否消费完
        for (TraceIdBucket traceIdBucket : TRACEID_BUCKET_LIST) {
            if (traceIdBucket.getBucketPos() != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 消费剩下的bucket
     */
    @Deprecated
    public static void consumeRemainBucket() {
        for (TraceIdBucket traceIdBucket : TRACEID_BUCKET_LIST) {
            logger.info("开始拉取最后剩余的数据......");
            int i = 0;
            boolean flag = false;
            while (traceIdBucket.getProcessCount() < PROCESS_COUNT && i < 5) {
                // 重试5次
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
                if (i == 4) {
                   flag = true;
                   traceIdBucket.clear();
                }
            }
            if (flag) continue;

            // 访问次数达到了请求client数据来消费
            String traceIdListString = JSON.toJSONString(traceIdBucket.getTraceIdList());
            int bucketPos = traceIdBucket.getBucketPos();
            String msg = String.format("{\"type\": %d, \"traceIdList\": %s, \"bucketPos\": %d}",
                    Constants.PULL_TRACE_DETAIL_TYPE, traceIdListString, bucketPos);
            channels.writeAndFlush(new TextWebSocketFrame(msg));
        }
    }

    public static long getStartTime(String span) {
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

}
