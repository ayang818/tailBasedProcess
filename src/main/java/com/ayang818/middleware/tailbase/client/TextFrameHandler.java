package com.ayang818.middleware.tailbase.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.ayang818.middleware.tailbase.Constants;
import com.ayang818.middleware.tailbase.utils.WsClient;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author 杨丰畅
 * @description TODO
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
                    JSONObject jsonObject = JSON.parseObject(payload);
                    int type = jsonObject.getObject("type", Integer.class);
                    logger.info("收到backend发送的消息......");
                    switch (type) {
                        case Constants.PULL_TRACE_DETAIL_TYPE:
                            logger.info("收到backend拉取bucket data 请求，开始拉取数据......");
                            List<String> wrongTraceIdList = jsonObject.getObject("traceIdList",
                                    new TypeReference<List<String>>() {});
                            Integer pos = jsonObject.getObject("pos", Integer.class);
                            String wrongTraceDetails =
                                    ClientDataStreamHandler.getWrongTracing(wrongTraceIdList,
                                            pos);
                            String msg = String.format("{\"type\": %d, \"data\": %s, " +
                                            "\"dataPos\": %d}",
                                    Constants.TRACE_DETAIL, wrongTraceDetails, pos);
                            WsClient.getWebSocketClient().sendTextFrame(msg);
                            logger.info("成功发送pos {} 请求拉取的traceDetail数据......", pos);
                            break;
                        default:
                            break;
                    }
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
