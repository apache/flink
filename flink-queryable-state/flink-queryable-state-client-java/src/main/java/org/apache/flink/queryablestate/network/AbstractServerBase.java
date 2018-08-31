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
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
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

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The base class for every server in the queryable state module.
 * It is using pure netty to send and receive messages of type {@link MessageBody}.
 *
 * @param <REQ> the type of request the server expects to receive.
 * @param <RESP> the type of response the server will send.
 */
@Internal
public abstract class AbstractServerBase<REQ extends MessageBody, RESP extends MessageBody> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	/** AbstractServerBase config: low water mark. */
	private static final int LOW_WATER_MARK = 8 * 1024;

	/** AbstractServerBase config: high water mark. */
	private static final int HIGH_WATER_MARK = 32 * 1024;

	/** The name of the server, useful for debugging. */
	private final String serverName;

	/** The {@link InetAddress address} to listen to. */
	private final InetAddress bindAddress;

	/** A port range on which to try to connect. */
	private final Set<Integer> bindPortRange;

	/** The number of threads to be allocated to the event loop. */
	private final int numEventLoopThreads;

	/** The number of threads to be used for query serving. */
	private final int numQueryThreads;

	/** Atomic shut down future. */
	private final AtomicReference<CompletableFuture<Void>> serverShutdownFuture = new AtomicReference<>(null);

	/** Netty's ServerBootstrap. */
	private ServerBootstrap bootstrap;

	/** Query executor thread pool. */
	private ExecutorService queryExecutor;

	/** Address of this server. */
	private InetSocketAddress serverAddress;

	/** The handler used for the incoming messages. */
	private AbstractServerHandler<REQ, RESP> handler;

	/**
	 * Creates the {@link AbstractServerBase}.
	 *
	 * <p>The server needs to be started via {@link #start()}.
	 *
	 * @param serverName the name of the server
	 * @param bindAddress address to bind to
	 * @param bindPortIterator port to bind to
	 * @param numEventLoopThreads number of event loop threads
	 */
	protected AbstractServerBase(
			final String serverName,
			final InetAddress bindAddress,
			final Iterator<Integer> bindPortIterator,
			final Integer numEventLoopThreads,
			final Integer numQueryThreads) {

		Preconditions.checkNotNull(bindPortIterator);
		Preconditions.checkArgument(numEventLoopThreads >= 1, "Non-positive number of event loop threads.");
		Preconditions.checkArgument(numQueryThreads >= 1, "Non-positive number of query threads.");

		this.serverName = Preconditions.checkNotNull(serverName);
		this.bindAddress = Preconditions.checkNotNull(bindAddress);
		this.numEventLoopThreads = numEventLoopThreads;
		this.numQueryThreads = numQueryThreads;

		this.bindPortRange = new HashSet<>();
		while (bindPortIterator.hasNext()) {
			int port = bindPortIterator.next();
			Preconditions.checkArgument(port >= 0 && port <= 65535,
					"Invalid port configuration. Port must be between 0 and 65535, but was " + port + ".");
			bindPortRange.add(port);
		}
	}

	/**
	 * Creates a thread pool for the query execution.
	 * @return Thread pool for query execution
	 */
	private ExecutorService createQueryExecutor() {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink " + getServerName() + " Thread %d")
				.build();
		return Executors.newFixedThreadPool(numQueryThreads, threadFactory);
	}

	/**
	 * Returns the thread-pool responsible for processing incoming requests.
	 */
	protected ExecutorService getQueryExecutor() {
		return queryExecutor;
	}

	/**
	 * Gets the name of the server. This is useful for debugging.
	 * @return The name of the server.
	 */
	public String getServerName() {
		return serverName;
	}

	/**
	 * Returns the {@link AbstractServerHandler handler} to be used for
	 * serving the incoming requests.
	 */
	public abstract AbstractServerHandler<REQ, RESP> initializeHandler();

	/**
	 * Returns the address of this server.
	 *
	 * @return AbstractServerBase address
	 * @throws IllegalStateException If server has not been started yet
	 */
	public InetSocketAddress getServerAddress() {
		Preconditions.checkState(serverAddress != null, "Server " + serverName + " has not been started.");
		return serverAddress;
	}

	/**
	 * Starts the server by binding to the configured bind address (blocking).
	 * @throws Exception If something goes wrong during the bind operation.
	 */
	public void start() throws Throwable {
		Preconditions.checkState(serverAddress == null && serverShutdownFuture.get() == null,
				serverName + " is already running @ " + serverAddress + ". ");

		Iterator<Integer> portIterator = bindPortRange.iterator();
		while (portIterator.hasNext() && !attemptToBind(portIterator.next())) {}

		if (serverAddress != null) {
			log.info("Started {} @ {}.", serverName, serverAddress);
		} else {
			log.info("Unable to start {}. All ports in provided range ({}) are occupied.", serverName, bindPortRange);
			throw new FlinkRuntimeException("Unable to start " + serverName + ". All ports in provided range are occupied.");
		}
	}

	/**
	 * Tries to start the server at the provided port.
	 *
	 * <p>This, in conjunction with {@link #start()}, try to start the
	 * server on a free port among the port range provided at the constructor.
	 *
	 * @param port the port to try to bind the server to.
	 * @throws Exception If something goes wrong during the bind operation.
	 */
	private boolean attemptToBind(final int port) throws Throwable {
		log.debug("Attempting to start {} on port {}.", serverName, port);

		this.queryExecutor = createQueryExecutor();
		this.handler = initializeHandler();

		final NettyBufferPool bufferPool = new NettyBufferPool(numEventLoopThreads);

		final ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setDaemon(true)
				.setNameFormat("Flink " + serverName + " EventLoop Thread %d")
				.build();

		final NioEventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);

		this.bootstrap = new ServerBootstrap()
				.localAddress(bindAddress, port)
				.group(nioGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.ALLOCATOR, bufferPool)
				.childOption(ChannelOption.ALLOCATOR, bufferPool)
				.childHandler(new ServerChannelInitializer<>(handler));

		final int defaultHighWaterMark = 64 * 1024; // from DefaultChannelConfig (not exposed)
		//noinspection ConstantConditions
		// (ignore warning here to make this flexible in case the configuration values change)
		if (LOW_WATER_MARK > defaultHighWaterMark) {
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK);
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK);
		} else { // including (newHighWaterMark < defaultLowWaterMark)
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, LOW_WATER_MARK);
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, HIGH_WATER_MARK);
		}

		try {
			final ChannelFuture future = bootstrap.bind().sync();
			if (future.isSuccess()) {
				final InetSocketAddress localAddress = (InetSocketAddress) future.channel().localAddress();
				serverAddress = new InetSocketAddress(localAddress.getAddress(), localAddress.getPort());
				return true;
			}

			// the following throw is to bypass Netty's "optimization magic"
			// and catch the bind exception.
			// the exception is thrown by the sync() call above.

			throw future.cause();
		} catch (BindException e) {
			log.debug("Failed to start {} on port {}: {}.", serverName, port, e.getMessage());
			try {
				// we shutdown the server but we reset the future every time because in
				// case of failure to bind, we will call attemptToBind() here, and not resetting
				// the flag will interfere with future shutdown attempts.

				shutdownServer()
						.whenComplete((ignoredV, ignoredT) -> serverShutdownFuture.getAndSet(null))
						.get();
			} catch (Exception r) {

				// Here we were seeing this problem:
				// https://github.com/netty/netty/issues/4357 if we do a get().
				// this is why we now simply wait a bit so that everything is shut down.

				log.warn("Problem while shutting down {}: {}", serverName, r.getMessage());
			}
		}
		// any other type of exception we let it bubble up.
		return false;
	}

	/**
	 * Shuts down the server and all related thread pools.
	 * @return A {@link CompletableFuture} that will be completed upon termination of the shutdown process.
	 */
	public CompletableFuture<Void> shutdownServer() {
		CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
		if (serverShutdownFuture.compareAndSet(null, shutdownFuture)) {
			log.info("Shutting down {} @ {}", serverName, serverAddress);

			final CompletableFuture<Void> groupShutdownFuture = new CompletableFuture<>();
			if (bootstrap != null) {
				EventLoopGroup group = bootstrap.group();
				if (group != null && !group.isShutdown()) {
					group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
							.addListener(finished -> {
								if (finished.isSuccess()) {
									groupShutdownFuture.complete(null);
								} else {
									groupShutdownFuture.completeExceptionally(finished.cause());
								}
							});
				} else {
					groupShutdownFuture.complete(null);
				}
			} else {
				groupShutdownFuture.complete(null);
			}

			final CompletableFuture<Void> handlerShutdownFuture = new CompletableFuture<>();
			if (handler == null) {
				handlerShutdownFuture.complete(null);
			} else {
				handler.shutdown().whenComplete((result, throwable) -> {
					if (throwable != null) {
						handlerShutdownFuture.completeExceptionally(throwable);
					} else {
						handlerShutdownFuture.complete(null);
					}
				});
			}

			final CompletableFuture<Void> queryExecShutdownFuture = CompletableFuture.runAsync(() -> {
				if (queryExecutor != null) {
					ExecutorUtils.gracefulShutdown(10L, TimeUnit.MINUTES, queryExecutor);
				}
			});

			CompletableFuture.allOf(
					queryExecShutdownFuture, groupShutdownFuture, handlerShutdownFuture
			).whenComplete((result, throwable) -> {
				if (throwable != null) {
					shutdownFuture.completeExceptionally(throwable);
				} else {
					shutdownFuture.complete(null);
				}
			});
		}
		return serverShutdownFuture.get();
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
	public boolean isEventGroupShutdown() {
		return bootstrap == null || bootstrap.group().isTerminated();
	}
}
