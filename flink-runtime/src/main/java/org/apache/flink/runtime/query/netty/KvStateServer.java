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

package org.apache.flink.runtime.query.netty;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.netty.message.KvStateRequest;
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
 * Netty-based server answering {@link KvStateRequest} messages.
 *
 * <p>Requests are handled by asynchronous query tasks (see {@link KvStateServerHandler.AsyncKvStateQueryTask})
 * that are executed by a separate query Thread pool. This pool is shared among
 * all TCP connections.
 *
 * <p>The incoming pipeline looks as follows:
 * <pre>
 * Socket.read() -> LengthFieldBasedFrameDecoder -> KvStateServerHandler
 * </pre>
 *
 * <p>Received binary messages are expected to contain a frame length field. Netty's
 * {@link LengthFieldBasedFrameDecoder} is used to fully receive the frame before
 * giving it to our {@link KvStateServerHandler}.
 *
 * <p>Connections are established and closed by the client. The server only
 * closes the connection on a fatal failure that cannot be recovered. A
 * server-side connection close is considered a failure by the client.
 */
public class KvStateServer {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateServer.class);

	/** Server config: low water mark. */
	private static final int LOW_WATER_MARK = 8 * 1024;

	/** Server config: high water mark. */
	private static final int HIGH_WATER_MARK = 32 * 1024;

	/** Netty's ServerBootstrap. */
	private final ServerBootstrap bootstrap;

	/** Query executor thread pool. */
	private final ExecutorService queryExecutor;

	/** Address of this server. */
	private KvStateServerAddress serverAddress;

	/**
	 * Creates the {@link KvStateServer}.
	 *
	 * <p>The server needs to be started via {@link #start()} in order to bind
	 * to the configured bind address.
	 *
	 * @param bindAddress         Address to bind to
	 * @param bindPort            Port to bind to. Pick random port if 0.
	 * @param numEventLoopThreads Number of event loop threads
	 * @param numQueryThreads     Number of query threads
	 * @param kvStateRegistry     KvStateRegistry to query for KvState instances
	 * @param stats               Statistics tracker
	 */
	public KvStateServer(
			InetAddress bindAddress,
			int bindPort,
			int numEventLoopThreads,
			int numQueryThreads,
			KvStateRegistry kvStateRegistry,
			KvStateRequestStats stats) {

		Preconditions.checkArgument(bindPort >= 0 && bindPort <= 65536, "Port " + bindPort +
				" is out of valid port range (0-65536).");

		Preconditions.checkArgument(numEventLoopThreads >= 1, "Non-positive number of event loop threads.");
		Preconditions.checkArgument(numQueryThreads >= 1, "Non-positive number of query threads.");

		Preconditions.checkNotNull(kvStateRegistry, "KvStateRegistry");
		Preconditions.checkNotNull(stats, "KvStateRequestStats");

		NettyBufferPool bufferPool = new NettyBufferPool(numEventLoopThreads);

		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink KvStateServer EventLoop Thread %d")
				.build();

		NioEventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);

		queryExecutor = createQueryExecutor(numQueryThreads);

		// Shared between all channels
		KvStateServerHandler serverHandler = new KvStateServerHandler(
				kvStateRegistry,
				queryExecutor,
				stats);

		bootstrap = new ServerBootstrap()
				// Bind address and port
				.localAddress(bindAddress, bindPort)
				// NIO server channels
				.group(nioGroup)
				.channel(NioServerSocketChannel.class)
				// Server channel Options
				.option(ChannelOption.ALLOCATOR, bufferPool)
				// Child channel options
				.childOption(ChannelOption.ALLOCATOR, bufferPool)
				.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK)
				.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK)
				// See initializer for pipeline details
				.childHandler(new KvStateServerChannelInitializer(serverHandler));
	}

	/**
	 * Starts the server by binding to the configured bind address (blocking).
	 *
	 * @throws InterruptedException If interrupted during the bind operation
	 */
	public void start() throws InterruptedException {
		Channel channel = bootstrap.bind().sync().channel();

		InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
		serverAddress = new KvStateServerAddress(localAddress.getAddress(), localAddress.getPort());
	}

	/**
	 * Returns the address of this server.
	 *
	 * @return Server address
	 * @throws IllegalStateException If server has not been started yet
	 */
	public KvStateServerAddress getAddress() {
		if (serverAddress == null) {
			throw new IllegalStateException("KvStateServer not started yet.");
		}

		return serverAddress;
	}

	/**
	 * Shuts down the server and all related thread pools.
	 */
	public void shutDown() {
		if (bootstrap != null) {
			EventLoopGroup group = bootstrap.group();
			if (group != null) {
				group.shutdownGracefully(0, 10, TimeUnit.SECONDS);
			}
		}

		if (queryExecutor != null) {
			queryExecutor.shutdown();
		}

		serverAddress = null;
	}

	/**
	 * Creates a thread pool for the query execution.
	 *
	 * @param numQueryThreads Number of query threads.
	 * @return Thread pool for query execution
	 */
	private static ExecutorService createQueryExecutor(int numQueryThreads) {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink KvStateServer Query Thread %d")
				.build();

		return Executors.newFixedThreadPool(numQueryThreads, threadFactory);
	}

	/**
	 * Channel pipeline initializer.
	 *
	 * <p>The request handler is shared, whereas the other handlers are created
	 * per channel.
	 */
	private static final class KvStateServerChannelInitializer extends ChannelInitializer<SocketChannel> {

		/** The shared request handler. */
		private final KvStateServerHandler sharedRequestHandler;

		/**
		 * Creates the channel pipeline initializer with the shared request handler.
		 *
		 * @param sharedRequestHandler Shared request handler.
		 */
		public KvStateServerChannelInitializer(KvStateServerHandler sharedRequestHandler) {
			this.sharedRequestHandler = Preconditions.checkNotNull(sharedRequestHandler, "Request handler");
		}

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			ch.pipeline()
					.addLast(new ChunkedWriteHandler())
					.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
					.addLast(sharedRequestHandler);
		}
	}

}
