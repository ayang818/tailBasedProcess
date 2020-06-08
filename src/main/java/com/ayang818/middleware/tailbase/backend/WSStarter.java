package com.ayang818.middleware.tailbase.backend;

import com.ayang818.middleware.tailbase.Constants;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 杨丰畅
 * @description 启动 websocket 服务
 * @date 2020/5/16 19:25
 **/
public class WSStarter {

    private static final Logger logger = LoggerFactory.getLogger(WSStarter.class);

    private ChannelFuture channelFuture;

    public static MessageHandler messageHandler;

    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap server = new ServerBootstrap();
        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new ChunkedWriteHandler());
                        pipeline.addLast(new HttpObjectAggregator( 100 * 1024 * 1024, false));

                        pipeline.addLast(new WebSocketServerProtocolHandler("/handle", null, false, 409600));
                        messageHandler = new MessageHandler();
                        pipeline.addLast(messageHandler);

                        // 60s 无读写，断开链接
                        pipeline.addLast(new IdleStateHandler(0, 0, 60));
                        pipeline.addLast(new IdleHandler());
                    }
                });

        channelFuture = server.bind(Constants.BACKEND_WEBSOCKET_PORT);
        channelFuture.addListener((ChannelFutureListener) future -> logger.info("websocket 服务已在 {} 端口启动", Constants.BACKEND_WEBSOCKET_PORT));
    }
}
