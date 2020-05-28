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
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ayang818.middleware.tailbase.Constants.TARGET_PROCESS_COUNT;
import static com.ayang818.middleware.tailbase.backend.PullDataService.resMap;

/**
 * @author 杨丰畅
 * @description 处理 websocket 信息
 * @date 2020/5/16 19:39
 **/
@ChannelHandler.Sharable
public class MessageHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

    private static ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private static final ExecutorService reportThreadPool = Executors.newFixedThreadPool(3,
            new DefaultThreadFactory("report-checkSum"));

    /**
     * FIN_TIME will add one
     */
    private static final AtomicInteger FIN_TIME = new AtomicInteger(0);

    /**
     * save 90 buckets for wrong trace
     */
    private static final int BUCKET_COUNT = 90;

    private static final List<TraceIdBucket> TRACEID_BUCKET_LIST = new ArrayList<>();

    /**
     * key is pos, value is data
     */
    private static final Map<String, ACKData> ACK_MAP = new ConcurrentHashMap<>(100);

    private static final Object EVEN_LOCK = new Object();

    private static final Object ODD_LOCK = new Object();

    private static final Object[] LOCK_LIST = new Object[BUCKET_COUNT];

    public static void init() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            TRACEID_BUCKET_LIST.add(new TraceIdBucket());
            LOCK_LIST[i] = new Object();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {
        String text = msg.text();
        JSONObject jsonObject = JSON.parseObject(text);
        Integer type = jsonObject.getObject("type", Integer.class);

        switch (type) {
            case Constants.UPDATE_TYPE:
                Set<String> badTraceIdSet = jsonObject.getObject("badTraceIdSet",
                        new TypeReference<Set<String>>() {});
                int pos = jsonObject.getObject("pos", Integer.class);
                // logger.info("收到client pos {} 的update", pos);

                setWrongTraceId(badTraceIdSet, pos);
                break;
            case Constants.TRACE_DETAIL:
                Map<String, List<String>> spans = jsonObject.getObject("data",
                    new TypeReference<Map<String, List<String>>>(){});
                // pull data from this pos, here is for a recent ack!
                int dataPos = jsonObject.getObject("dataPos", Integer.class);
                // logger.info("收到client pos {} 的traceDetail", dataPos);
                // 消费
                consumeTraceDetails(spans, dataPos);
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
     *
     * @param detailMap
     * @param pos
     */
    private void consumeTraceDetails(Map<String, List<String>> detailMap, final Integer pos) {
        String posStr = String.valueOf(pos);
        ACKData ackData;
        int remainAccessTime;

        // 以免重复检测到未放入，导致脏读（一组，减少竞争）
        synchronized (LOCK_LIST[pos % BUCKET_COUNT]) {
            if (ACK_MAP.containsKey(posStr)) {
                ackData = ACK_MAP.get(posStr);
            } else {
                ackData = new ACKData();
                ACK_MAP.put(posStr, ackData);
            }
            remainAccessTime = ackData.putAll(detailMap);
        }

        // 剩余可访问次数
        // logger.info("pos {} 处的数据还可被访问 {} 次", pos, remainAccessTime);

        if (remainAccessTime == 0) {
            final Map<String, List<String>> map = ackData.getAckMap();

            reportThreadPool.execute(() -> {
                // 这里的key为string，计算md5
                for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                    String spans = entry.getValue()
                            .stream()
                            .sorted(Comparator.comparing(MessageHandler::getStartTime))
                            .collect(Collectors.joining("\n"));
                    spans += "\n";
                    //String res = "";
                    //res += entry.getKey() + ": " +entry.getValue().size() + "\n";
                    //try {
                    //    Files.write(Paths.get("D:/middlewaredata/my.data"), (res+spans).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    //} catch (IOException e) {
                    //    e.printStackTrace();
                    //}
                    resMap.put(entry.getKey(), md5(spans));
                }

                // 此时才可以还原 bucketPos 所在 bucket 为初始状态，因为这个时候才刚好处理完这个bucket
                TRACEID_BUCKET_LIST.get(pos % BUCKET_COUNT).clear();
                ACK_MAP.remove(posStr);

                logger.info("{} 处的bucket消费完毕，清空此处的 bucket", pos);

                logger.info("当前检测到 {} 条错误链路", resMap.size());
            });

        }
    }

    /**
     * 向client发送拉取数据的请求
     * @param traceIdListString
     * @param pos
     */
    public static void pullWrongTraceDetails(String traceIdListString, int pos) {
        String msg = String.format("{\"type\": %d, \"traceIdSet\": %s, \"pos\": %d}",
                Constants.PULL_TRACE_DETAIL_TYPE, traceIdListString, pos);
        channels.writeAndFlush(new TextWebSocketFrame(msg));
        logger.info("发送拉取 {} pos处 bucket data请求.....", pos);
    }

    /**
     * 将client主动推送过来的错误traceIdList，放置到backend 等待消费的 对应位置的 bucket 中
     * @param badTraceIdSet
     * @param pos
     */
    public void setWrongTraceId(Set<String> badTraceIdSet, int pos) {
        int bucketPos = pos % BUCKET_COUNT;
        TraceIdBucket traceIdBucket = TRACEID_BUCKET_LIST.get(bucketPos);
        if (traceIdBucket.getPos() != 0 && traceIdBucket.getPos() != pos) {
            logger.warn("覆盖了 {} 位置的正在工作的 bucket!!!", bucketPos);
        }
        if (badTraceIdSet != null && badTraceIdSet.size() > 0) {
            traceIdBucket.setPos(pos);
            int processCount = traceIdBucket.addProcessCount();
            traceIdBucket.getTraceIdSet().addAll(badTraceIdSet);
            logger.info(String.format("pos %d 位置的 bucket 访问次数到达 %d", pos, processCount));
            // 使用阻塞队列优化，如果processCount >= TARGET_PROCESS_COUNT，那么推入消费队列，等待消费
            if (processCount >= TARGET_PROCESS_COUNT) {
               PullDataService.blockingQueue.offer(traceIdBucket);
            }
        }
    }

    /**
     * 是否结束，是否可以向评测程序发送答案
     * @return
     */
    public static boolean isFin() {
        // 是否收到对应次FIN信号
        if (FIN_TIME.get() < TARGET_PROCESS_COUNT) return false;
        // bucket中元素是否消费完
        for (TraceIdBucket traceIdBucket : TRACEID_BUCKET_LIST) {
            if (traceIdBucket.getPos() != 0) {
                return false;
            }
        }
        return true;
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

    /**
     * 获取一个可以被消费的bucket
     * @param startPos 上一次被消费的bucket所在的位置 + 1，既然上一个位置刚刚被消费，那么下一个位置可以被消费的概率也很大，
     *                 而且可以及时释放client端对应bucket
     * @return
     */
    @Deprecated
    public static TraceIdBucket getFinishedBucket(int startPos) {
        int end = startPos + BUCKET_COUNT;
        for (int i = startPos; i < end; i++) {
            int cur = i % BUCKET_COUNT;
            TraceIdBucket currentBucket = TRACEID_BUCKET_LIST.get(cur);

            if (currentBucket.getProcessCount() >= TARGET_PROCESS_COUNT) {
                return currentBucket;
            }
        }
        return null;
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
            while (traceIdBucket.getProcessCount() < TARGET_PROCESS_COUNT && i < 5) {
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
            String traceIdListString = JSON.toJSONString(traceIdBucket.getTraceIdSet());
            int bucketPos = traceIdBucket.getPos();
            String msg = String.format("{\"type\": %d, \"traceIdSet\": %s, \"bucketPos\": %d}",
                    Constants.PULL_TRACE_DETAIL_TYPE, traceIdListString, bucketPos);
            channels.writeAndFlush(new TextWebSocketFrame(msg));
        }
    }

}
