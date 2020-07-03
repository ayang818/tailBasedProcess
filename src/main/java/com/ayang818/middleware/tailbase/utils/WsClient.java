package com.ayang818.middleware.tailbase.utils;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.ayang818.middleware.tailbase.client.TextFrameHandler.wsHandler;

/**
 * @author 杨丰畅
 * @description 建立与 backend 的 websocket 连接
 * @date 2020/5/14 20:19
 **/
public class WsClient {

    private static final Logger logger = LoggerFactory.getLogger(WsClient.class);

    private static final DefaultAsyncHttpClientConfig config =
            Dsl.config()
                    .setWebSocketMaxFrameSize(4096000)
                    .setEventLoopGroup(new NioEventLoopGroup(1, new DefaultThreadFactory(
                            "pool-websocket")))
                    .build();

    private static final AsyncHttpClient client = Dsl.asyncHttpClient(config);

    private static volatile WebSocket receiveWebsocketClient;

    private static Object rLock = new Object();

    public static WebSocket getWebsocketClient() {
        if (receiveWebsocketClient == null) {
            synchronized (rLock) {
                if (receiveWebsocketClient == null) {
                    try {
                        receiveWebsocketClient = client
                                .prepareGet("ws://localhost:8003/handle")
                                .setRequestTimeout(10000)
                                .execute(wsHandler)
                                .get();
                        // *info("ws接收长连接建立成功......");
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return receiveWebsocketClient;
    }

}
