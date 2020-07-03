package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
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

import static com.ayang818.middleware.tailbase.client.DataStorage.DETAIL_THREAD;

/**
 * @author 杨丰畅
 * @description client 端 websocket 信息处理器
 * @date 2020/5/22 21:59
 **/
public class TextFrameHandler {

    private static final Logger logger = LoggerFactory.getLogger(TextFrameHandler.class);

    private static final StringBuilder msgBuilder = new StringBuilder(750000);

    public static WebSocketUpgradeHandler wsHandler = new WebSocketUpgradeHandler.Builder()
            .addWebSocketListener(new WebSocketListener() {
                @Override
                public void onOpen(WebSocket websocket) {
                    // WebSocket connection opened
                    // *info("websocket 连接已建立......");
                }

                @Override
                public void onTextFrame(String payload, boolean finalFragment, int rsv) {
                    // backend向client拉取具体信息
                    Caller caller = JSON.parseObject(payload, new TypeReference<Caller>() {
                    });
                    Set<String> errTraceIdSet;
                    int pos;
                    List<Caller.PullDataBucket> pullDataBucketList = caller.getData();
                    List<Resp> data = new ArrayList<>();
                    int len = pullDataBucketList.size();
                    for (int i = 0; i < len; i++) {
                        Caller.PullDataBucket pullDataBucket = pullDataBucketList.get(i);
                        errTraceIdSet = pullDataBucket.getErrTraceIdSet();
                        pos = pullDataBucket.getPos();
                        ClientDataStreamHandler.getWrongTracing(errTraceIdSet,
                                pos, data);
                    }
                    // json格式 { "type": Constants.TRACE_DETAIL "data": [ {"data": %s, "dataPos": %d} ] }
                    DETAIL_THREAD.execute(() -> {
                        msgBuilder.append("{\"type\":")
                                .append(Constants.TRACE_DETAIL)
                                .append(", \"data\":")
                                .append(JSON.toJSONString(data))
                                .append("}");
                        ClientDataStreamHandler.websocket.sendTextFrame(msgBuilder.toString());
                        msgBuilder.delete(0, msgBuilder.length());
                    });
                    // *info("收集 pos {} 前的数据结束", pullDataBucketList.get(len - 1).pos);
                }

                @Override
                public void onClose(WebSocket websocket, int code, String reason) {
                    // WebSocket connection closed
                    // *info("websocket 连接已断开......");
                }

                @Override
                public void onError(Throwable t) {
                    // WebSocket connection error
                    // *error("websocket 连接发生错误，堆栈信息如下......");
                    t.printStackTrace();
                }

            }).build();

}
