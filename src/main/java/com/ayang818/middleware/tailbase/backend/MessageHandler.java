package com.ayang818.middleware.tailbase.backend;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.common.Caller;
import com.ayang818.middleware.tailbase.common.Resp;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ayang818.middleware.tailbase.Constants.BACKEND_BUCKET_COUNT;
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
    private static final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    // FIN_TIME will add one
    private static final AtomicInteger FIN_TIME = new AtomicInteger(0);
    // waiting to consume bucket list
    private static final List<TraceIdBucket> TRACEID_BUCKET_LIST = new ArrayList<>(BACKEND_BUCKET_COUNT);
    // waiting client's ack data; key is pos, value is data
    private static final Map<String, ACKData> ACK_MAP = new ConcurrentHashMap<>(200);
    // consume thread
    private static final ExecutorService consumeThread = Executors.newSingleThreadExecutor(new DefaultThreadFactory("consume-thread"));
    private static final ExecutorService updateThread = Executors.newSingleThreadExecutor(new DefaultThreadFactory("update-thread"));
    // lock list
    private static volatile int consumePos = 0;
    private static volatile int updatePos = 0;
    // use for md5 calc
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static void init() {
        for (int i = 0; i < BACKEND_BUCKET_COUNT; i++) {
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
                updateThread.execute(() -> {
                    List<String> data = jsonObject.getObject("data", new TypeReference<List<String>>(){});
                    Set<String> badTraceIdSet;
                    int pos;
                    JSONObject tmp;
                    for (String updateDto : data) {
                        tmp = JSON.parseObject(updateDto);
                        badTraceIdSet = tmp.getObject("badTraceIdSet", new TypeReference<Set<String>>(){});
                        pos = tmp.getObject("pos", Integer.class);
                        setWrongTraceId(badTraceIdSet, pos);
                    }
                });
                break;
            case Constants.TRACE_DETAIL:
                consumeThread.execute(() -> {
                    List<Resp> respDetails = jsonObject.getObject("data",
                            new TypeReference<List<Resp>>() {
                            });
                    for (Resp resp : respDetails) {
                        // pull data from this pos, here is for a recent ack!
                        consumeTraceDetails(resp.getData(), resp.getDataPos());
                    }
                });
                break;
            case Constants.FIN_TYPE:
                int finTime = FIN_TIME.addAndGet(1);
                // *info("收到 {} 次 Fin请求", finTime);
                break;
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
    private void consumeTraceDetails(Map<String, List<String>> detailMap, int pos) {
        String posStr = String.valueOf(pos);
        ACKData ackData;
        int remainAccessTime = -1;
        // 以免重复检测到未放入，导致脏读（一组锁，减少竞争）
        int p = pos % BACKEND_BUCKET_COUNT;
        ackData = ACK_MAP.computeIfAbsent(posStr, (v) -> new ACKData());
        remainAccessTime = ackData.putAll(detailMap);

        if (remainAccessTime == 0) {
            Map<String, List<String>> ackMap = ackData.getAckMap();
            // 这里的key为string，计算md5
            Iterator<Map.Entry<String, List<String>>> it = ackMap.entrySet().iterator();
            Map.Entry<String, List<String>> entry;
            while (it.hasNext()) {
                entry = it.next();
                String spans = entry.getValue()
                        .stream()
                        .sorted(Comparator.comparing(MessageHandler::getStartTime))
                        .collect(Collectors.joining("\n"));
                spans += "\n";
                resMap.put(entry.getKey(), md5(spans));
            }
            // 此时才可以还原 bucketPos 所在 bucket 为初始状态，因为这个时候才刚好处理完这个bucket
            TraceIdBucket traceIdBucket = TRACEID_BUCKET_LIST.get(p);
            traceIdBucket.reset();
            ACK_MAP.remove(posStr);
            // *info("{} {} 处的bucket消费完毕，清空此处的 bucket, size {}; 当前检测到 {} 条错误链路", p, pos, traceIdBucket.getTraceIdSet().size(), resMap.size());
        }
        consumePos = Math.max(consumePos, pos);
    }

    /**
     * 批量拉取数据
     *
     * @param traceIdBucketList
     */
    public static void pullWrongTraceDetails(List<Caller.PullDataBucket> traceIdBucketList) {
        Caller caller = new Caller(Constants.PULL_TRACE_DETAIL_TYPE, traceIdBucketList);
        String msg = JSON.toJSONString(caller);
        channels.writeAndFlush(new TextWebSocketFrame(msg));
        List<Caller.PullDataBucket> data = caller.data;
        // *info("发送拉取bucket {}前 data请求.....", data.get(data.size() - 1).pos);
    }

    /**
     * 将client主动推送过来的错误traceIdList，放置到backend 等待消费的 对应位置的 bucket 中
     *
     * @param badTraceIdSet
     * @param pos
     */
    public void setWrongTraceId(Set<String> badTraceIdSet, int pos) {
        int bucketPos = pos % BACKEND_BUCKET_COUNT;
        TraceIdBucket traceIdBucket = TRACEID_BUCKET_LIST.get(bucketPos);
        int curPos = traceIdBucket.getPos();
        if (curPos != -1 && curPos != pos) {
            while (traceIdBucket.getPos() != -1) {
                try {
                    Thread.sleep(1);
                    // *info("等待 pos {} 被消费, consumePos {}, updatePos {}", pos, consumePos, updatePos);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // 不管这一区间是否有错误链路，都需要添加
        traceIdBucket.setPos(pos);
        int processCount = traceIdBucket.addProcessCount();
        traceIdBucket.getTraceIdSet().addAll(badTraceIdSet);
        // 使用阻塞队列优化，如果processCount >= TARGET_PROCESS_COUNT，那么推入消费队列，等待消费
        if (processCount >= TARGET_PROCESS_COUNT) {
            TraceIdBucket finalTraceIdBucket = new TraceIdBucket();
            finalTraceIdBucket.setPos(traceIdBucket.getPos());
            finalTraceIdBucket.getTraceIdSet().addAll(traceIdBucket.getTraceIdSet());
            PullDataService.blockingQueue.offer(finalTraceIdBucket);
        }
        updatePos = Math.max(updatePos, pos);
    }

    /**
     * 是否结束，是否可以向评测程序发送答案
     *
     * @return
     */
    public static boolean isFin() {
        // 是否收到对应次FIN信号
        if (FIN_TIME.get() < TARGET_PROCESS_COUNT) return false;
        // bucket中元素是否消费完
        for (TraceIdBucket traceIdBucket : TRACEID_BUCKET_LIST) {
            if (traceIdBucket.getPos() != -1) {
                // *info("{} pos处仍在等待", traceIdBucket.getPos());
                return false;
            }
        }
        return true;
    }

    public static long getStartTime(String span) {
        StringBuilder startTimeBuilder = new StringBuilder();
        char[] chars = span.toCharArray();
        int ICount = 0;
        int startTimeStartPos = 0;
        int len = chars.length;
        for (int i = 0; i < len; i++) {
            if (chars[i] == '|') {
                ICount += 1;
                if (ICount == 1) {
                    startTimeStartPos = i + 1;
                }
                if (ICount == 2) {
                    startTimeBuilder.append(chars, startTimeStartPos, i - startTimeStartPos);
                    return BaseUtils.toLong(startTimeBuilder.toString(), -1);
                }
            }
        }
        return -1;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        channels.add(ctx.channel());
        // *info("添加一条新的连接，当前总共 {} 条连接......", channels.size());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        channels.remove(ctx.channel());
        // *info("删除一条连接，当前总共 {} 条连接......", channels.size());
    }

    public String md5(String key) {
        try {
            byte[] btInput = key.getBytes(StandardCharsets.UTF_8);
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
                str[k++] = HEX_DIGITS[byte0 >>> 4 & 0xf];
                str[k++] = HEX_DIGITS[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            return null;
        }
    }
}
