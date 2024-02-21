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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import jdk.net.ExtendedSocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketOption;

import static org.apache.flink.util.Preconditions.checkState;

class NettyClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    @VisibleForTesting static final String NIO_TCP_KEEPIDLE_KEY = "TCP_KEEPIDLE";
    @VisibleForTesting static final String NIO_TCP_KEEPINTERVAL_KEY = "TCP_KEEPINTERVAL";
    @VisibleForTesting static final String NIO_TCP_KEEPCOUNT_KEY = "TCP_KEEPCOUNT";

    private final NettyConfig config;

    private NettyProtocol protocol;

    private Bootstrap bootstrap;

    @Nullable private SSLHandlerFactory clientSSLFactory;

    NettyClient(NettyConfig config) {
        this.config = config;
    }

    void init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
        checkState(bootstrap == null, "Netty client has already been initialized.");

        this.protocol = protocol;

        final long start = System.nanoTime();

        bootstrap = new Bootstrap();

        // --------------------------------------------------------------------
        // Transport-specific configuration
        // --------------------------------------------------------------------

        switch (config.getTransportType()) {
            case NIO:
                initNioBootstrap();
                break;

            case EPOLL:
                initEpollBootstrap();
                break;

            case AUTO:
                if (Epoll.isAvailable()) {
                    initEpollBootstrap();
                    LOG.info("Transport type 'auto': using EPOLL.");
                } else {
                    initNioBootstrap();
                    LOG.info("Transport type 'auto': using NIO.");
                }
        }

        // --------------------------------------------------------------------
        // Configuration
        // --------------------------------------------------------------------

        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

        // Timeout for new connections
        bootstrap.option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                config.getClientConnectTimeoutSeconds() * 1000);

        // Pooled allocator for Netty's ByteBuf instances
        bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);

        // Receive and send buffer size
        int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
        if (receiveAndSendBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
            bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
        }

        try {
            clientSSLFactory = config.createClientSSLEngineFactory();
        } catch (Exception e) {
            throw new IOException("Failed to initialize SSL Context for the Netty client", e);
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful initialization (took {} ms).", duration);
    }

    NettyConfig getConfig() {
        return config;
    }

    Bootstrap getBootstrap() {
        return bootstrap;
    }

    void shutdown() {
        final long start = System.nanoTime();

        if (bootstrap != null) {
            if (bootstrap.config().group() != null) {
                bootstrap.config().group().shutdownGracefully();
            }
            bootstrap = null;
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful shutdown (took {} ms).", duration);
    }

    private void initNioBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.
        String name =
                NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPortRange() + ")";

        NioEventLoopGroup nioGroup =
                new NioEventLoopGroup(
                        config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
        bootstrap.group(nioGroup).channel(NioSocketChannel.class);

        config.getTcpKeepIdleInSeconds()
                .ifPresent(idle -> setNioKeepaliveOptions(NIO_TCP_KEEPIDLE_KEY, idle));
        config.getTcpKeepInternalInSeconds()
                .ifPresent(interval -> setNioKeepaliveOptions(NIO_TCP_KEEPINTERVAL_KEY, interval));
        config.getTcpKeepCount()
                .ifPresent(count -> setNioKeepaliveOptions(NIO_TCP_KEEPCOUNT_KEY, count));
    }

    @SuppressWarnings("unchecked")
    private void setNioKeepaliveOptions(String option, int value) {
        try {
            Field field = ExtendedSocketOptions.class.getField(option);
            bootstrap.option(NioChannelOption.of((SocketOption<Integer>) field.get(null)), value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOG.error(
                    "Ignore keepalive option {}, this may be due to using netty transport type of nio and an older version of jdk 8,"
                            + " refer to https://bugs.openjdk.org/browse/JDK-8194298",
                    option,
                    e);
        }
    }

    private void initEpollBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.
        String name =
                NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPortRange() + ")";

        EpollEventLoopGroup epollGroup =
                new EpollEventLoopGroup(
                        config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
        bootstrap.group(epollGroup).channel(EpollSocketChannel.class);

        config.getTcpKeepIdleInSeconds()
                .ifPresent(idle -> bootstrap.option(EpollChannelOption.TCP_KEEPIDLE, idle));
        config.getTcpKeepInternalInSeconds()
                .ifPresent(
                        interval -> bootstrap.option(EpollChannelOption.TCP_KEEPINTVL, interval));
        config.getTcpKeepCount()
                .ifPresent(count -> bootstrap.option(EpollChannelOption.TCP_KEEPCNT, count));
    }

    // ------------------------------------------------------------------------
    // Client connections
    // ------------------------------------------------------------------------

    ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
        checkState(bootstrap != null, "Client has not been initialized yet.");

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------

        bootstrap.handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) throws Exception {

                        // SSL handler should be added first in the pipeline
                        if (clientSSLFactory != null) {
                            SslHandler sslHandler =
                                    clientSSLFactory.createNettySSLHandler(
                                            channel.alloc(),
                                            serverSocketAddress.getAddress().getCanonicalHostName(),
                                            serverSocketAddress.getPort());
                            channel.pipeline().addLast("ssl", sslHandler);
                        }
                        channel.pipeline().addLast(protocol.getClientChannelHandlers());
                    }
                });

        try {
            return bootstrap.connect(serverSocketAddress);
        } catch (ChannelException e) {
            if ((e.getCause() instanceof java.net.SocketException
                            && e.getCause().getMessage().equals("Too many open files"))
                    || (e.getCause() instanceof ChannelException
                            && e.getCause().getCause() instanceof java.net.SocketException
                            && e.getCause()
                                    .getCause()
                                    .getMessage()
                                    .equals("Too many open files"))) {
                throw new ChannelException(
                        "The operating system does not offer enough file handles to open the network connection. "
                                + "Please increase the number of available file handles.",
                        e.getCause());
            } else {
                throw e;
            }
        }
    }
}
