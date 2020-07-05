package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.backend.MessageHandler;
import com.ayang818.middleware.tailbase.backend.PullDataService;
import com.ayang818.middleware.tailbase.backend.WSStarter;
import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * <p>application starter</p>
 *
 * @author : chengyi
 * @date : 2020-06-11 20:35
 **/
public class MainStarter {
    private static final Logger logger = LoggerFactory.getLogger(MainStarter.class);
    private static String port;

    public static void main(String[] args) {
        // *info(Arrays.toString(args));
        port = System.getProperty("server.port", "8000");
        // *info("init port={}", port);
        if (BaseUtils.isBackendProcess()) {
            MessageHandler.init();
            // start websocket service
            new WSStarter().run();
            // start consume thread
            PullDataService.start();
            // *info("数据后端已在 {} 端口启动......", port);
        }
        if (BaseUtils.isClientProcess()) {
            ClientDataStreamHandler.init();
            // *info("数据客户端已在 {} 端口启动......", port);
        }
        // start this application
        MainStarter.start();
    }

    private static void start() {
        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup work = new NioEventLoopGroup(1);
        bootstrap.group(boss, work)
                // .handler(new LoggingHandler(LogLevel.INFO))
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast("httpAggregator", new HttpObjectAggregator(512 * 1024)); // http 消息聚合器                                                                     512*1024为接收的最大contentlength
                        pipeline.addLast(new BasicHttpHandler());
                    }
                });

        port = System.getProperty("server.port", "8000");
        try {
            ChannelFuture f = bootstrap.bind(new InetSocketAddress(Integer.parseInt(port))).sync();
            f.channel().closeFuture().sync();
            System.out.println(" application start up on port : " + port);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
