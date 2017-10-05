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

package org.apache.flink.queryablestate.network;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The base class for every server in the queryable state module.
 * It is using pure netty to send and receive messages of type {@link MessageBody}.
 *
 * @param <REQ> the type of request the server expects to receive.
 * @param <RESP> the type of response the server will send.
 */
@Internal
public abstract class AbstractServerBase<REQ extends MessageBody, RESP extends MessageBody> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractServerBase.class);

	/** AbstractServerBase config: low water mark. */
	private static final int LOW_WATER_MARK = 8 * 1024;

	/** AbstractServerBase config: high water mark. */
	private static final int HIGH_WATER_MARK = 32 * 1024;

	private final String serverName;

	/** Netty's ServerBootstrap. */
	private final ServerBootstrap bootstrap;

	/** Query executor thread pool. */
	private final ExecutorService queryExecutor;

	/** Address of this server. */
	private KvStateServerAddress serverAddress;

	/** The handler used for the incoming messages. */
	private AbstractServerHandler<REQ, RESP> handler;

	/**
	 * Creates the {@link AbstractServerBase}.
	 *
	 * <p>The server needs to be started via {@link #start()} in order to bind
	 * to the configured bind address.
	 *
	 * @param serverName the name of the server
	 * @param bindAddress address to bind to
	 * @param bindPort port to bind to (random port if 0)
	 * @param numEventLoopThreads number of event loop threads
	 */
	protected AbstractServerBase(
			final String serverName,
			final InetAddress bindAddress,
			final Integer bindPort,
			final Integer numEventLoopThreads,
			final Integer numQueryThreads) {

		Preconditions.checkNotNull(bindAddress);
		Preconditions.checkArgument(bindPort >= 0 && bindPort <= 65536, "Port " + bindPort + " out of valid range (0-65536).");
		Preconditions.checkArgument(numEventLoopThreads >= 1, "Non-positive number of event loop threads.");
		Preconditions.checkArgument(numQueryThreads >= 1, "Non-positive number of query threads.");

		this.serverName = Preconditions.checkNotNull(serverName);
		this.queryExecutor = createQueryExecutor(numQueryThreads);

		final NettyBufferPool bufferPool = new NettyBufferPool(numEventLoopThreads);

		final ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink " + serverName + " EventLoop Thread %d")
				.build();

		final NioEventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);

		bootstrap = new ServerBootstrap()
				// Bind address and port
				.localAddress(bindAddress, bindPort)
				// NIO server channels
				.group(nioGroup)
				.channel(NioServerSocketChannel.class)
				// AbstractServerBase channel Options
				.option(ChannelOption.ALLOCATOR, bufferPool)
				// Child channel options
				.childOption(ChannelOption.ALLOCATOR, bufferPool)
				.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK)
				.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK);
	}

	/**
	 * Creates a thread pool for the query execution.
	 *
	 * @param numQueryThreads Number of query threads.
	 * @return Thread pool for query execution
	 */
	private ExecutorService createQueryExecutor(int numQueryThreads) {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink " + getServerName() + " Thread %d")
				.build();

		return Executors.newFixedThreadPool(numQueryThreads, threadFactory);
	}

	protected ExecutorService getQueryExecutor() {
		return queryExecutor;
	}

	public String getServerName() {
		return serverName;
	}

	public abstract AbstractServerHandler<REQ, RESP> initializeHandler();

	/**
	 * Starts the server by binding to the configured bind address (blocking).
	 * @throws InterruptedException If interrupted during the bind operation
	 */
	public void start() throws InterruptedException {
		Preconditions.checkState(serverAddress == null,
				"Server " + serverName + " has already been started @ " + serverAddress + '.');

		this.handler = initializeHandler();
		bootstrap.childHandler(new ServerChannelInitializer<>(handler));

		Channel channel = bootstrap.bind().sync().channel();
		InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
		serverAddress = new KvStateServerAddress(localAddress.getAddress(), localAddress.getPort());

		LOG.info("Started server {} @ {}", serverName, serverAddress);
	}

	/**
	 * Returns the address of this server.
	 *
	 * @return AbstractServerBase address
	 * @throws IllegalStateException If server has not been started yet
	 */
	public KvStateServerAddress getServerAddress() {
		Preconditions.checkState(serverAddress != null, "Server " + serverName + " has not been started.");
		return serverAddress;
	}

	/**
	 * Shuts down the server and all related thread pools.
	 */
	public void shutdown() {
		LOG.info("Shutting down server {} @ {}", serverName, serverAddress);

		if (handler != null) {
			handler.shutdown();
		}

		if (queryExecutor != null) {
			queryExecutor.shutdown();
		}

		if (bootstrap != null) {
			EventLoopGroup group = bootstrap.group();
			if (group != null) {
				group.shutdownGracefully(0L, 10L, TimeUnit.SECONDS);
			}
		}
		serverAddress = null;
	}

	/**
	 * Channel pipeline initializer.
	 *
	 * <p>The request handler is shared, whereas the other handlers are created
	 * per channel.
	 */
	private static final class ServerChannelInitializer<REQ extends MessageBody, RESP extends MessageBody> extends ChannelInitializer<SocketChannel> {

		/** The shared request handler. */
		private final AbstractServerHandler<REQ, RESP> sharedRequestHandler;

		/**
		 * Creates the channel pipeline initializer with the shared request handler.
		 *
		 * @param sharedRequestHandler Shared request handler.
		 */
		ServerChannelInitializer(AbstractServerHandler<REQ, RESP> sharedRequestHandler) {
			this.sharedRequestHandler = Preconditions.checkNotNull(sharedRequestHandler, "MessageBody handler");
		}

		@Override
		protected void initChannel(SocketChannel channel) throws Exception {
			channel.pipeline()
					.addLast(new ChunkedWriteHandler())
					.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
					.addLast(sharedRequestHandler);
		}
	}

	@VisibleForTesting
	public boolean isExecutorShutdown() {
		return queryExecutor.isShutdown();
	}
}
