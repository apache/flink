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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

class NettyServer {

	private static final ThreadFactoryBuilder THREAD_FACTORY_BUILDER = new ThreadFactoryBuilder().setDaemon(true);

	private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

	private final NettyConfig config;

	private ServerBootstrap bootstrap;

	private ChannelFuture bindFuture;

	NettyServer(NettyConfig config) {
		this.config = checkNotNull(config);
	}

	void init(final NettyProtocol protocol) throws IOException {
		checkState(bootstrap == null, "Netty server has already been initialized.");

		long start = System.currentTimeMillis();

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
		bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		if (config.getServerConnectBacklog() > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
		}

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

		// Low and high water marks for flow control
		bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, config.getMemorySegmentSize() + 1);
		bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 2 * config.getMemorySegmentSize());

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {
				protocol.setServerChannelPipeline(channel.pipeline());
			}
		});

		// --------------------------------------------------------------------
		// Start Server
		// --------------------------------------------------------------------

		bindFuture = bootstrap.bind().syncUninterruptibly();

		long end = System.currentTimeMillis();
		LOG.info("Successful initialization  (took {} ms). Listening on SocketAddress {}.", (end - start), bindFuture.channel().localAddress().toString());
	}

	void shutdown() {
		long start = System.currentTimeMillis();
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
		long end = System.currentTimeMillis();
		LOG.info("Successful shutdown (took {} ms).", (end - start));
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
}
