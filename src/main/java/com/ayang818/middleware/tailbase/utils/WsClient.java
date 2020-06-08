package com.ayang818.middleware.tailbase.utils;

import com.ayang818.middleware.tailbase.Constants;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

import static com.ayang818.middleware.tailbase.client.TextFrameHandler.wsHandler;

/**
 * @author 杨丰畅
 * @description 建立与 backend 的 websocket 连接
 * @date 2020/5/14 20:19
 **/
@Component
public class WsClient {

    private static final Logger logger = LoggerFactory.getLogger(WsClient.class);

    private static DefaultAsyncHttpClientConfig sendConfig =
            Dsl.config()
                    .setWebSocketMaxFrameSize(204800)
                    .setEventLoopGroup(new NioEventLoopGroup(1, new DefaultThreadFactory(
                            "send-websocket")))
                    .build();

    private static DefaultAsyncHttpClientConfig receiveConfig =
            Dsl.config()
                    .setWebSocketMaxFrameSize(204800)
                    .setEventLoopGroup(new NioEventLoopGroup(4, new DefaultThreadFactory(
                            "receive-websocket")))
                    .build();

    private static AsyncHttpClient sendClient = Dsl.asyncHttpClient(sendConfig);

    private static AsyncHttpClient receiveClient = Dsl.asyncHttpClient(receiveConfig);

    private static volatile WebSocket sendWebsocketClient;

    private static volatile WebSocket receiveWebsocketClient;

    private static Object sLock = new Object();

    private static Object rLock = new Object();

    public static WebSocket getSendWebsocketClient() {
        if (sendWebsocketClient == null) {
            synchronized (sLock) {
                if (sendWebsocketClient == null) {
                    try {
                        sendWebsocketClient = sendClient
                                .prepareGet("ws://localhost:8003/handle")
                                .setRequestTimeout(10000)
                                .execute(new WebSocketUpgradeHandler.Builder()
                                        .addWebSocketListener(new WebSocketListener() {
                                            @Override
                                            public void onOpen(WebSocket webSocket) {
                                            }
                                            @Override
                                            public void onClose(WebSocket webSocket, int i, String s) {
                                            }
                                            @Override
                                            public void onError(Throwable throwable) {
                                            }
                                        }).build())
                                .get();
                        logger.info("ws发送长连接建立成功......");
                        sendWebsocketClient.sendTextFrame(String.format("{\"type\": %d, \"channelType\": %d}", Constants.CHANNEL_TYPE, Constants.SENDER_TYPE));
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return sendWebsocketClient;
    }

    public static WebSocket getReceiveWebsocketClient() {
        if (receiveWebsocketClient == null) {
            synchronized (rLock) {
                if (receiveWebsocketClient == null) {
                    try {
                        receiveWebsocketClient = receiveClient
                                .prepareGet("ws://localhost:8003/handle")
                                .setRequestTimeout(10000)
                                .execute(wsHandler)
                                .get();
                        logger.info("ws接收长连接建立成功......");
                        receiveWebsocketClient.sendTextFrame(String.format("{\"type\": %d, \"channelType\": %d}", Constants.CHANNEL_TYPE, Constants.RECEIVER_TYPE));
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return receiveWebsocketClient;
    }

}
