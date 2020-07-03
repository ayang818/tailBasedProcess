package com.ayang818.middleware.tailbase;

import com.ayang818.middleware.tailbase.client.ClientDataStreamHandler;
import com.ayang818.middleware.tailbase.utils.BaseUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>http api handler </p>
 *
 * @author : chengyi
 * @date : 2020-06-11 20:51
 **/
public class BasicHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final Logger logger = LoggerFactory.getLogger(BasicHttpHandler.class);

    private static volatile String port = null;

    public static String getDataSourcePort() {
        return port;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String url = request.uri();
        String requestMethod = request.method().name();
        if (url.startsWith("/ready") && "GET".equals(requestMethod)) {
            handleReady(ctx, url, requestMethod);
        }
        if (url.startsWith("/setParameter") && "GET".equals(requestMethod)) {
            handleSetParameter(ctx, url, requestMethod);
        }
    }

    private void handleSetParameter(ChannelHandlerContext ctx, String url, String requestMethod) {
        if (url.contains("port")) {
            port = url.substring(url.indexOf("=") + 1);
            // *info(port);
            // *info("set port {}", port);
        } else {
            // *info("can not get port");
        }
        if (BaseUtils.isClientProcess()) {
            // start to handle data Stream
            ClientDataStreamHandler.start();
        }
        ctx.writeAndFlush(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer("", CharsetUtil.UTF_8)));
    }

    private void handleReady(ChannelHandlerContext ctx, String url, String requestMethod) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer("", CharsetUtil.UTF_8)));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
        // *info("{}", cause.getMessage());
    }
}
