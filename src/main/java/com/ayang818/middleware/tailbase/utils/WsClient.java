package com.ayang818.middleware.tailbase.utils;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.ws.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import static com.ayang818.middleware.tailbase.client.TextFrameHandler.*;

/**
 * @author 杨丰畅
 * @description TODO
 * @date 2020/5/14 20:19
 **/
@Component
public class WsClient {

    private static final Logger logger = LoggerFactory.getLogger(WsClient.class);

    private static AsyncHttpClient client = Dsl.asyncHttpClient(Dsl.config().setWebSocketMaxFrameSize(204800).build());

    private static volatile WebSocket webSocketClient;

    static {
        try {
            webSocketClient = client
                        .prepareGet("ws://localhost:8003/handle")
                        .setRequestTimeout(10000)
                                .execute(wsHandler)
                                .get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static WebSocket getWebSocketClient() {
        return webSocketClient;
    }

}
