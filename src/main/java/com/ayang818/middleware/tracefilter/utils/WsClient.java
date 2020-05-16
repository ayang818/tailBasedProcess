package com.ayang818.middleware.tracefilter.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/14 20:19
 **/
public class WsClient {

    private static final Logger logger = LoggerFactory.getLogger(WsClient.class);

    private static volatile WebSocket webSocketClient = null;

    public static WebSocket getWebSocketClient(WebSocketUpgradeHandler wsHandler) {
        if (webSocketClient == null) {
            synchronized (WsClient.class) {
                if (webSocketClient == null) {
                        // 数据中心http端口为8002，ws端口为8003
                    try {
                        webSocketClient = Dsl.asyncHttpClient()
                                .prepareGet("ws://localhost:8003/handle")
                                .setRequestTimeout(5000)
                                .execute(wsHandler)
                                .get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    return webSocketClient;
                }
            }
        }
        return webSocketClient;
    }

}
