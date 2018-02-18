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

package org.apache.flink.runtime.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.RouterHandler;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Handler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An abstract class for netty-based REST server endpoints.
 */
public abstract class RestServerEndpoint {

	public static final int MAX_REQUEST_SIZE_BYTES = 1024 * 1024 * 10;
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final Object lock = new Object();

	private final String configuredAddress;
	private final int configuredPort;
	private final SSLEngine sslEngine;
	private final Path uploadDir;

	private final CompletableFuture<Void> terminationFuture;

	private ServerBootstrap bootstrap;
	private Channel serverChannel;
	private String restAddress;

	private State state = State.CREATED;

	public RestServerEndpoint(RestServerEndpointConfiguration configuration) throws IOException {
		Preconditions.checkNotNull(configuration);
		this.configuredAddress = configuration.getEndpointBindAddress();
		this.configuredPort = configuration.getEndpointBindPort();
		this.sslEngine = configuration.getSslEngine();

		this.uploadDir = configuration.getUploadDir();
		createUploadDir(uploadDir, log);

		terminationFuture = new CompletableFuture<>();

		this.restAddress = null;
	}

	/**
	 * This method is called at the beginning of {@link #start()} to setup all handlers that the REST server endpoint
	 * implementation requires.
	 *
	 * @param restAddressFuture future rest address of the RestServerEndpoint
	 * @return Collection of AbstractRestHandler which are added to the server endpoint
	 */
	protected abstract List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture);

	/**
	 * Starts this REST server endpoint.
	 *
	 * @throws Exception if we cannot start the RestServerEndpoint
	 */
	public final void start() throws Exception {
		synchronized (lock) {
			Preconditions.checkState(state == State.CREATED, "The RestServerEndpoint cannot be restarted.");

			log.info("Starting rest endpoint.");

			final Router router = new Router();
			final CompletableFuture<String> restAddressFuture = new CompletableFuture<>();

			List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = initializeHandlers(restAddressFuture);

			/* sort the handlers such that they are ordered the following:
			 * /jobs
			 * /jobs/overview
			 * /jobs/:jobid
			 * /jobs/:jobid/config
			 * /:*
			 */
			Collections.sort(
				handlers,
				RestHandlerUrlComparator.INSTANCE);

			handlers.forEach(handler -> {
				log.debug("Register handler {} under {}@{}.", handler.f1, handler.f0.getHttpMethod(), handler.f0.getTargetRestEndpointURL());
				registerHandler(router, handler);
			});

			ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {

				@Override
				protected void initChannel(SocketChannel ch) {
					Handler handler = new RouterHandler(router);

					// SSL should be the first handler in the pipeline
					if (sslEngine != null) {
						ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
					}

					ch.pipeline()
						.addLast(new HttpServerCodec())
						.addLast(new FileUploadHandler(uploadDir))
						.addLast(new HttpObjectAggregator(MAX_REQUEST_SIZE_BYTES))
						.addLast(handler.name(), handler)
						.addLast(new PipelineErrorHandler(log));
				}
			};

			NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("flink-rest-server-netty-boss"));
			NioEventLoopGroup workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("flink-rest-server-netty-worker"));

			bootstrap = new ServerBootstrap();
			bootstrap
				.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(initializer);

			final ChannelFuture channel;
			if (configuredAddress == null) {
				channel = bootstrap.bind(configuredPort);
			} else {
				channel = bootstrap.bind(configuredAddress, configuredPort);
			}
			serverChannel = channel.syncUninterruptibly().channel();

			InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
			String address = bindAddress.getAddress().getHostAddress();
			int port = bindAddress.getPort();

			log.info("Rest endpoint listening at {}:{}", address, port);

			final String protocol;

			if (sslEngine != null) {
				protocol = "https://";
			} else {
				protocol = "http://";
			}

			restAddress = protocol + address + ':' + port;

			restAddressFuture.complete(restAddress);

			state = State.RUNNING;

			startInternal();
		}
	}

	/**
	 * Hook to start sub class specific services.
	 *
	 * @throws Exception if an error occurred
	 */
	protected abstract void startInternal() throws Exception;

	/**
	 * Returns the address on which this endpoint is accepting requests.
	 *
	 * @return address on which this endpoint is accepting requests or null if none
	 */
	@Nullable
	public InetSocketAddress getServerAddress() {
		synchronized (lock) {
			Preconditions.checkState(state != State.CREATED, "The RestServerEndpoint has not been started yet.");
			Channel server = this.serverChannel;

			if (server != null) {
				try {
					return ((InetSocketAddress) server.localAddress());
				} catch (Exception e) {
					log.error("Cannot access local server address", e);
				}
			}

			return null;
		}
	}

	/**
	 * Returns the address of the REST server endpoint.
	 *
	 * @return REST address of this endpoint
	 */
	public String getRestAddress() {
		synchronized (lock) {
			Preconditions.checkState(state != State.CREATED, "The RestServerEndpoint has not been started yet.");
			return restAddress;
		}
	}

	public final CompletableFuture<Void> shutDownAsync() {
		synchronized (lock) {
			log.info("Shutting down rest endpoint.");

			if (state == State.RUNNING) {
				final CompletableFuture<Void> shutDownFuture = shutDownInternal();

				shutDownFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
				state = State.SHUTDOWN;
			} else if (state == State.CREATED) {
				terminationFuture.complete(null);
				state = State.SHUTDOWN;
			}

			return terminationFuture;
		}
	}

	/**
	 * Stops this REST server endpoint.
	 *
	 * @return Future which is completed once the shut down has been finished.
	 */
	protected CompletableFuture<Void> shutDownInternal() {

		synchronized (lock) {

			CompletableFuture<?> channelFuture = new CompletableFuture<>();
			if (serverChannel != null) {
				serverChannel.close().addListener(finished -> {
					if (finished.isSuccess()) {
						channelFuture.complete(null);
					} else {
						channelFuture.completeExceptionally(finished.cause());
					}
				});
				serverChannel = null;
			}
			CompletableFuture<?> groupFuture = new CompletableFuture<>();
			CompletableFuture<?> childGroupFuture = new CompletableFuture<>();
			final Time gracePeriod = Time.seconds(10L);

			channelFuture.thenRun(() -> {
				if (bootstrap != null) {
					if (bootstrap.group() != null) {
						bootstrap.group().shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
							.addListener(finished -> {
								if (finished.isSuccess()) {
									groupFuture.complete(null);
								} else {
									groupFuture.completeExceptionally(finished.cause());
								}
							});
					}
					if (bootstrap.childGroup() != null) {
						bootstrap.childGroup().shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
							.addListener(finished -> {
								if (finished.isSuccess()) {
									childGroupFuture.complete(null);
								} else {
									childGroupFuture.completeExceptionally(finished.cause());
								}
							});
					}
					bootstrap = null;
				} else {
					// complete the group futures since there is nothing to stop
					groupFuture.complete(null);
					childGroupFuture.complete(null);
				}
			});

			final CompletableFuture<Void> channelTerminationFuture = FutureUtils.completeAll(
				Arrays.asList(groupFuture, childGroupFuture));

			return FutureUtils.runAfterwards(
				channelTerminationFuture,
				() -> {
					log.info("Cleaning upload directory {}", uploadDir);
					FileUtils.cleanDirectory(uploadDir.toFile());
				});
		}
	}

	private static void registerHandler(Router router, Tuple2<RestHandlerSpecification, ChannelInboundHandler> specificationHandler) {
		switch (specificationHandler.f0.getHttpMethod()) {
			case GET:
				router.GET(specificationHandler.f0.getTargetRestEndpointURL(), specificationHandler.f1);
				break;
			case POST:
				router.POST(specificationHandler.f0.getTargetRestEndpointURL(), specificationHandler.f1);
				break;
			case DELETE:
				router.DELETE(specificationHandler.f0.getTargetRestEndpointURL(), specificationHandler.f1);
				break;
			case PATCH:
				router.PATCH(specificationHandler.f0.getTargetRestEndpointURL(), specificationHandler.f1);
				break;
			default:
				throw new RuntimeException("Unsupported http method: " + specificationHandler.f0.getHttpMethod() + '.');
		}
	}

	/**
	 * Creates the upload dir if needed.
	 */
	@VisibleForTesting
	static void createUploadDir(final Path uploadDir, final Logger log) throws IOException {
		if (!Files.exists(uploadDir)) {
			log.warn("Upload directory {} does not exist, or has been deleted externally. " +
				"Previously uploaded files are no longer available.", uploadDir);
			checkAndCreateUploadDir(uploadDir, log);
		}
	}

	/**
	 * Checks whether the given directory exists and is writable. If it doesn't exist, this method
	 * will attempt to create it.
	 *
	 * @param uploadDir directory to check
	 * @param log logger used for logging output
	 * @throws IOException if the directory does not exist and cannot be created, or if the
	 *                     directory isn't writable
	 */
	private static synchronized void checkAndCreateUploadDir(final Path uploadDir, final Logger log) throws IOException {
		if (Files.exists(uploadDir) && Files.isWritable(uploadDir)) {
			log.info("Using directory {} for file uploads.", uploadDir);
		} else if (Files.isWritable(Files.createDirectories(uploadDir))) {
			log.info("Created directory {} for file uploads.", uploadDir);
		} else {
			log.warn("Upload directory {} cannot be created or is not writable.", uploadDir);
			throw new IOException(
				String.format("Upload directory %s cannot be created or is not writable.",
					uploadDir));
		}
	}

	/**
	 * Comparator for Rest URLs.
	 *
	 * <p>The comparator orders the Rest URLs such that URLs with path parameters are ordered behind
	 * those without parameters. E.g.:
	 * /jobs
	 * /jobs/overview
	 * /jobs/:jobid
	 * /jobs/:jobid/config
	 * /:*
	 *
	 * <p>IMPORTANT: This comparator is highly specific to how Netty path parameters are encoded. Namely
	 * with a preceding ':' character.
	 */
	public static final class RestHandlerUrlComparator implements Comparator<Tuple2<RestHandlerSpecification, ChannelInboundHandler>>, Serializable {

		private static final long serialVersionUID = 2388466767835547926L;

		private static final Comparator<String> CASE_INSENSITIVE_ORDER = new CaseInsensitiveOrderComparator();

		static final RestHandlerUrlComparator INSTANCE = new RestHandlerUrlComparator();

		@Override
		public int compare(
				Tuple2<RestHandlerSpecification, ChannelInboundHandler> o1,
				Tuple2<RestHandlerSpecification, ChannelInboundHandler> o2) {
			return CASE_INSENSITIVE_ORDER.compare(o1.f0.getTargetRestEndpointURL(), o2.f0.getTargetRestEndpointURL());
		}

		/**
		 * Comparator for Rest URLs.
		 *
		 * <p>The comparator orders the Rest URLs such that URLs with path parameters are ordered behind
		 * those without parameters. E.g.:
		 * /jobs
		 * /jobs/overview
		 * /jobs/:jobid
		 * /jobs/:jobid/config
		 * /:*
		 *
		 * <p>IMPORTANT: This comparator is highly specific to how Netty path parameters are encoded. Namely
		 * with a preceding ':' character.
		 */
		public static final class CaseInsensitiveOrderComparator implements Comparator<String>, Serializable {
			private static final long serialVersionUID = 8550835445193437027L;

			@Override
			public int compare(String s1, String s2) {
				int n1 = s1.length();
				int n2 = s2.length();
				int min = Math.min(n1, n2);
				for (int i = 0; i < min; i++) {
					char c1 = s1.charAt(i);
					char c2 = s2.charAt(i);
					if (c1 != c2) {
						c1 = Character.toUpperCase(c1);
						c2 = Character.toUpperCase(c2);
						if (c1 != c2) {
							c1 = Character.toLowerCase(c1);
							c2 = Character.toLowerCase(c2);
							if (c1 != c2) {
								if (c1 == ':') {
									// c2 is less than c1 because it is also different
									return 1;
								} else if (c2 == ':') {
									// c1 is less than c2
									return -1;
								} else {
									return c1 - c2;
								}
							}
						}
					}
				}
				return n1 - n2;
			}
		}
	}

	private enum State {
		CREATED,
		RUNNING,
		SHUTDOWN
	}
}
