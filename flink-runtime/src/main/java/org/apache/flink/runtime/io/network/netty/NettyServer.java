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
import org.apache.flink.runtime.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class NettyServer {

	private static final ThreadFactoryBuilder THREAD_FACTORY_BUILDER =
		new ThreadFactoryBuilder()
			.setDaemon(true)
			.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);

	private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

	private final NettyConfig config;

	private ServerBootstrap bootstrap;

	private ChannelFuture bindFuture;

	private InetSocketAddress localAddress;

	NettyServer(NettyConfig config) {
		this.config = checkNotNull(config);
		localAddress = null;
	}

	int init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
		return init(
			nettyBufferPool,
			sslHandlerFactory -> new ServerChannelInitializer(protocol, sslHandlerFactory));
	}

	int init(
			NettyBufferPool nettyBufferPool,
			Function<SSLHandlerFactory, ServerChannelInitializer> channelInitializer) throws IOException {
		checkState(bootstrap == null, "Netty server has already been initialized.");

		final long start = System.nanoTime();

		bootstrap = new ServerBootstrap();

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
				}
				else {
					initNioBootstrap();
					LOG.info("Transport type 'auto': using NIO.");
				}
		}

		// --------------------------------------------------------------------
		// Configuration
		// --------------------------------------------------------------------

		// Server bind address
		bootstrap.localAddress(config.getServerAddress(), config.getServerPort());

		// Pooled allocators for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
		bootstrap.childOption(ChannelOption.ALLOCATOR, nettyBufferPool);

		if (config.getServerConnectBacklog() > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
		}

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

		// SSL related configuration
		final SSLHandlerFactory sslHandlerFactory;
		try {
			sslHandlerFactory = config.createServerSSLEngineFactory();
		} catch (Exception e) {
			throw new IOException("Failed to initialize SSL Context for the Netty Server", e);
		}

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------

		bootstrap.childHandler(channelInitializer.apply(sslHandlerFactory));

		// --------------------------------------------------------------------
		// Start Server
		// --------------------------------------------------------------------

		bindFuture = bootstrap.bind().syncUninterruptibly();

		localAddress = (InetSocketAddress) bindFuture.channel().localAddress();

		final long duration = (System.nanoTime() - start) / 1_000_000;
		LOG.info("Successful initialization (took {} ms). Listening on SocketAddress {}.", duration, localAddress);

		return localAddress.getPort();
	}

	NettyConfig getConfig() {
		return config;
	}

	ServerBootstrap getBootstrap() {
		return bootstrap;
	}

	void shutdown() {
		final long start = System.nanoTime();
		if (bindFuture != null) {
			bindFuture.channel().close().awaitUninterruptibly();
			bindFuture = null;
		}

		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}
		final long duration = (System.nanoTime() - start) / 1_000_000;
		LOG.info("Successful shutdown (took {} ms).", duration);
	}

	private void initNioBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple servers running on the same host.
		String name = NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
		bootstrap.group(nioGroup).channel(NioServerSocketChannel.class);
	}

	private void initEpollBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple servers running on the same host.
		String name = NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
		bootstrap.group(epollGroup).channel(EpollServerSocketChannel.class);
	}

	public static ThreadFactory getNamedThreadFactory(String name) {
		return THREAD_FACTORY_BUILDER.setNameFormat(name + " Thread %d").build();
	}

	@VisibleForTesting
	static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
		private final NettyProtocol protocol;
		private final SSLHandlerFactory sslHandlerFactory;

		public ServerChannelInitializer(
			NettyProtocol protocol, SSLHandlerFactory sslHandlerFactory) {
			this.protocol = protocol;
			this.sslHandlerFactory = sslHandlerFactory;
		}

		@Override
		public void initChannel(SocketChannel channel) throws Exception {
			if (sslHandlerFactory != null) {
				channel.pipeline().addLast("ssl",
					sslHandlerFactory.createNettySSLHandler(channel.alloc()));
			}

			channel.pipeline().addLast(protocol.getServerChannelHandlers());
		}
	}
}
