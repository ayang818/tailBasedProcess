package com.ayang818.middleware.tailbase.utils;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
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




        private static DefaultAsyncHttpClientConfig config =
                Dsl.config()
                        .setWebSocketMaxFrameSize(204800)
                        .setEventLoopGroup(new NioEventLoopGroup(8, new DefaultThreadFactory(
                                "async-http-threadPool")))
                        .build();
        private static AsyncHttpClient client = Dsl.asyncHttpClient(config);


    private static volatile WebSocket webSocketClient;

    public static void init() {
        try {
            webSocketClient = client
                    .prepareGet("ws://localhost:8003/handle")
                    .setRequestTimeout(10000)
                    .execute(wsHandler)
                    .get();
            logger.info("ws长连接建立成功......");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static WebSocket getWebSocketClient() {
        return webSocketClient;
    }

}
