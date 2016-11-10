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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Low-level Netty client.
 */
public class NettyClient {

	private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

	private final NettyConfig config;

	private NettyProtocol protocol;

	private Bootstrap bootstrap;

	public NettyClient(NettyConfig config) {
		this.config = config;
	}

	/**
	 * Initialize the client without a protocol.
	 *
	 * In lieu of a protocol, provide a channel handler at connect time.
	*/
	public void init(ByteBufAllocator nettyBufferPool) throws IOException {
		init(null, nettyBufferPool);
	}

	/**
	 * Initialize the client.
	 */
	public void init(final NettyProtocol protocol, ByteBufAllocator nettyBufferPool) throws IOException {
		checkState(bootstrap == null, "Netty client has already been initialized.");

		this.protocol = protocol;

		long start = System.currentTimeMillis();

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
				}
				else {
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
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getClientConnectTimeoutSeconds() * 1000);

		// Pooled allocator for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

        // --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------
		ChannelHandler handler;
		if(protocol != null && (handler = protocol.getClientChannelHandler()) != null) {
			bootstrap.handler(handler);
		}

		long end = System.currentTimeMillis();
		LOG.info("Successful initialization (took {} ms).", (end - start));
	}

	public NettyConfig getConfig() {
		return config;
	}

	Bootstrap getBootstrap() {
		return bootstrap;
	}

	/**
	 * Shutdown the Netty client.
	 */
	public void shutdown() {
		long start = System.currentTimeMillis();

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
		String name = config.getClientThreadGroupName();
		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(nioGroup).channel(NioSocketChannel.class);
	}

	private void initEpollBootstrap() {
		String name = config.getClientThreadGroupName();
		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
	}

	// ------------------------------------------------------------------------
	// Client connections
	// ------------------------------------------------------------------------

	/**
	 * Return a new promise using the event loop group associated with the client.
	 */
	public <T> Promise<T> newPromise() {
		return bootstrap.group().next().<T>newPromise();
	}

	/**
	 * Connect to the given server address.
     */
	public ChannelFuture connect(SocketAddress serverSocketAddress) {
		return connect(serverSocketAddress, null);
	}

	/**
	 * Connect to the given server address.
	 * @param handler the channel handler to use (overrides any protocol set during initialization).
	 */
	public ChannelFuture connect(SocketAddress serverSocketAddress, ChannelHandler handler) {
		checkState(bootstrap != null, "Client has not been initialized yet.");

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------
		try {
			if(handler != null) {
				return bootstrap.clone().handler(handler).connect(serverSocketAddress);
			}
			else {
				return bootstrap.connect(serverSocketAddress);
			}
		}
		catch (io.netty.channel.ChannelException e) {
			if ( (e.getCause() instanceof java.net.SocketException &&
					e.getCause().getMessage().equals("Too many open files")) ||
				(e.getCause() instanceof io.netty.channel.ChannelException &&
						e.getCause().getCause() instanceof java.net.SocketException &&
						e.getCause().getCause().getMessage().equals("Too many open files")))
			{
				throw new io.netty.channel.ChannelException(
						"The operating system does not offer enough file handles to open the network connection. " +
								"Please increase the number of of available file handles.", e.getCause());
			}
			else {
				throw e;
			}
		}
	}
}
