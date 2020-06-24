package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.common.Caller;
import com.ayang818.middleware.tailbase.common.Resp;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author 杨丰畅
 * @description client 端 websocket 信息处理器
 * @date 2020/5/22 21:59
 **/
public class TextFrameHandler {

    private static final Logger logger = LoggerFactory.getLogger(TextFrameHandler.class);

    public static WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    logger.info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    long start = System.nanoTime();
                    // backend向client拉取信息
                    Caller caller = JSON.parseObject(payload, new TypeReference<Caller>(){});
                    Set<String> errTraceIdSet;
                    int pos;
                    List<Caller.PullDataBucket> pullDataBucketList = caller.getData();
                    List<Resp> data = new ArrayList<>();
                    for (Caller.PullDataBucket pullDataBucket : pullDataBucketList) {
                        errTraceIdSet = pullDataBucket.getErrTraceIdSet();
                        pos = pullDataBucket.getPos();
                        ClientDataStreamHandler.getWrongTracing(errTraceIdSet,
                                        pos, data);
                    }
                    // json格式 { "type": Constants.TRACE_DETAIL "data": [ {"data": %s, "dataPos": %d} ] }
                    String msg = String.format("{\"type\": %d, \"data\": %s}",
                            Constants.TRACE_DETAIL, JSON.toJSONString(data));
                    ClientDataStreamHandler.websocket.sendTextFrame(msg);
                    ClientDataStreamHandler.sum += (System.nanoTime() - start);
                    ClientDataStreamHandler.time++;
                    logger.info("total cost={}ns, avg={}ns", ClientDataStreamHandler.sum, ClientDataStreamHandler.sum / ClientDataStreamHandler.time);
                }

                @Override
                public void onClose(WebSocket websocket, int code, String reason) {
                    // WebSocket connection closed
                    logger.info("websocket 连接已断开......");
                }

                @Override
                public void onError(Throwable t) {
                    // WebSocket connection error
                    logger.error("websocket 连接发生错误，堆栈信息如下......");
                    t.printStackTrace();
                }

            }).build();

}
