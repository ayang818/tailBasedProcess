package com.ayang818.middleware.tailbase.backend;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.springframework.stereotype.Component;

/**
 * @author 杨丰畅
 * @description 超时心跳检测
 * @date 2020/5/16 19:42
 **/
@ChannelHandler.Sharable
@Component
public class IdleHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.ALL_IDLE) {
                WSStarter.messageHandler.handlerRemoved(ctx);
            }
        }
    }
}
