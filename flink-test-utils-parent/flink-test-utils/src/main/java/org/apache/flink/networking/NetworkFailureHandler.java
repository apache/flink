/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.networking;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handler that is forwarding inbound traffic from the source channel to the target channel on
 * remoteHost:remotePort and the responses in the opposite direction. All of the network traffic can
 * be blocked at any time using blocked flag.
 */
class NetworkFailureHandler extends SimpleChannelUpstreamHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkFailureHandler.class);
    private static final String TARGET_CHANNEL_HANDLER_NAME = "target_channel_handler";

    // mapping between source and target channels, used for finding correct target channel to use
    // for given source.
    private final Map<Channel, Channel> sourceToTargetChannels = new ConcurrentHashMap<>();
    private final Consumer<NetworkFailureHandler> onClose;
    private final ClientSocketChannelFactory channelFactory;
    private final String remoteHost;
    private final int remotePort;
    private final AtomicBoolean blocked;
    // The set of channels that are being closed in closeOnFlush(). This is needed to avoid
    // closing a channel recursively. See FLINK-22085 for more detail.
    private static final Set<Channel> channelsBeingClosed = ConcurrentHashMap.newKeySet();

    private static final ChannelFutureListener CLOSE_WITH_BOOKKEEPING =
            new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) {
                    future.getChannel()
                            .close()
                            .addListener(
                                    channelFuture ->
                                            channelsBeingClosed.remove(channelFuture.getChannel()));
                }
            };

    public NetworkFailureHandler(
            AtomicBoolean blocked,
            Consumer<NetworkFailureHandler> onClose,
            ClientSocketChannelFactory channelFactory,
            String remoteHost,
            int remotePort) {
        this.blocked = blocked;
        this.onClose = onClose;
        this.channelFactory = channelFactory;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    /** Closes the specified channel after all queued write requests are flushed. */
    static void closeOnFlush(Channel channel) {
        if (channel.isConnected() && !channelsBeingClosed.contains(channel)) {
            channelsBeingClosed.add(channel);
            channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(CLOSE_WITH_BOOKKEEPING);
        }
    }

    public void closeConnections() {
        for (Map.Entry<Channel, Channel> entry : sourceToTargetChannels.entrySet()) {
            // target channel is closed on source's channel channelClosed even
            entry.getKey().close();
        }
    }

    @Override
    public void channelOpen(ChannelHandlerContext context, ChannelStateEvent event)
            throws Exception {
        // Suspend incoming traffic until connected to the remote host.
        final Channel sourceChannel = event.getChannel();
        sourceChannel.setReadable(false);

        boolean isBlocked = blocked.get();
        LOG.debug(
                "Attempt to open proxy channel from [{}] to [{}:{}] in state [blocked = {}]",
                sourceChannel.getLocalAddress(),
                remoteHost,
                remotePort,
                isBlocked);

        if (isBlocked) {
            sourceChannel.close();
            return;
        }

        // Start the connection attempt.
        ClientBootstrap targetConnectionBootstrap = new ClientBootstrap(channelFactory);
        targetConnectionBootstrap
                .getPipeline()
                .addLast(
                        TARGET_CHANNEL_HANDLER_NAME,
                        new TargetChannelHandler(event.getChannel(), blocked));
        ChannelFuture connectFuture =
                targetConnectionBootstrap.connect(new InetSocketAddress(remoteHost, remotePort));
        sourceToTargetChannels.put(sourceChannel, connectFuture.getChannel());

        connectFuture.addListener(
                future -> {
                    if (future.isSuccess()) {
                        // Connection attempt succeeded:
                        // Begin to accept incoming traffic.
                        sourceChannel.setReadable(true);
                    } else {
                        // Close the connection if the connection attempt has failed.
                        sourceChannel.close();
                    }
                });
    }

    @Override
    public void messageReceived(ChannelHandlerContext context, MessageEvent event)
            throws Exception {
        if (blocked.get()) {
            return;
        }

        ChannelBuffer msg = (ChannelBuffer) event.getMessage();
        Channel targetChannel = sourceToTargetChannels.get(event.getChannel());
        if (targetChannel == null) {
            throw new IllegalStateException(
                    "Could not find a target channel for the source channel");
        }
        targetChannel.write(msg);
    }

    @Override
    public void channelClosed(ChannelHandlerContext context, ChannelStateEvent event)
            throws Exception {
        Channel targetChannel = sourceToTargetChannels.get(event.getChannel());
        if (targetChannel == null) {
            return;
        }
        closeOnFlush(targetChannel);
        sourceToTargetChannels.remove(event.getChannel());
        onClose.accept(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event)
            throws Exception {
        LOG.error("Closing communication channel because of an exception", event.getCause());
        closeOnFlush(event.getChannel());
    }

    private static class TargetChannelHandler extends SimpleChannelUpstreamHandler {
        private final Channel sourceChannel;
        private final AtomicBoolean blocked;

        TargetChannelHandler(Channel sourceChannel, AtomicBoolean blocked) {
            this.sourceChannel = sourceChannel;
            this.blocked = blocked;
        }

        @Override
        public void messageReceived(ChannelHandlerContext context, MessageEvent event)
                throws Exception {
            if (blocked.get()) {
                return;
            }
            ChannelBuffer msg = (ChannelBuffer) event.getMessage();
            sourceChannel.write(msg);
        }

        @Override
        public void channelClosed(ChannelHandlerContext context, ChannelStateEvent event)
                throws Exception {
            closeOnFlush(sourceChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, ExceptionEvent event)
                throws Exception {
            LOG.error("Closing communication channel because of an exception", event.getCause());
            closeOnFlush(event.getChannel());
        }
    }
}
