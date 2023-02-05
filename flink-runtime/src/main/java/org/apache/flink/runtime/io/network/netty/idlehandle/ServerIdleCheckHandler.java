package org.apache.flink.runtime.io.network.netty.idlehandle;


import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class ServerIdleCheckHandler extends IdleStateHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerIdleCheckHandler.class);

    public ServerIdleCheckHandler(Long readerIdleTime) {
        super(readerIdleTime, 0, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
        if (evt == IdleStateEvent.READER_IDLE_STATE_EVENT) {
            LOG.warn(
                    "server idle check happen, timeout {} millisecond",
                    super.getReaderIdleTimeInMillis()
            );
            return;
        }

        super.channelIdle(ctx, evt);
    }
}
