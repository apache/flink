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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;

class NettyClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

	private final NettyConfig config;

	private Bootstrap bootstrap;

	NettyClient(NettyConfig config) {
		this.config = config;
	}

	void init(final NettyProtocol protocol) throws IOException {
		if (bootstrap != null) {
			throw new IOException("NettyClient has already been initialized.");
		}

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
					LOGGER.info("Transport type 'auto': using EPOLL.");
				}
				else {
					initNioBootstrap();
					LOGGER.info("Transport type 'auto': using NIO.");
				}
		}

		// --------------------------------------------------------------------
		// Configuration
		// --------------------------------------------------------------------

		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

		// Timeout for new connections
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getClientConnectTimeoutMs());

		// Pooled allocator for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		// Low and high water marks for flow control
		bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, config.getMemorySegmentSize() / 2);
		bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, config.getMemorySegmentSize());

		// Receive and send buffer size
		if (config.getSendAndReceiveBufferSize() > 0) {
			int size = config.getSendAndReceiveBufferSize();

			bootstrap.option(ChannelOption.SO_SNDBUF, size);
			bootstrap.option(ChannelOption.SO_RCVBUF, size);
		}

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {
				protocol.setClientChannelPipeline(channel.pipeline());
			}
		});

		LOGGER.info("Successful initialization.");
	}

	ChannelFuture connect(SocketAddress remoteAddress) throws IOException {
		if (bootstrap == null) {
			throw new IOException("NettyClient has not been initialized yet.");
		}

		return bootstrap.connect(remoteAddress);
	}

	void shutdown() {
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}

		LOGGER.info("Successful shutdown.");
	}

	private void initNioBootstrap() {
		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getClientNumThreads(), Util.getNamedThreadFactory(NettyConfig.CLIENT_THREAD_GROUP_NAME));
		bootstrap.group(nioGroup).channel(NioSocketChannel.class);
	}

	private void initEpollBootstrap() {
		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getServerNumThreads(), Util.getNamedThreadFactory(NettyConfig.CLIENT_THREAD_GROUP_NAME));
		bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
	}
}
