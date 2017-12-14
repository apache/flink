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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.RouterHandler;
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

import javax.net.ssl.SSLEngine;

import java.io.Serializable;
import java.net.InetSocketAddress;
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

	private ServerBootstrap bootstrap;
	private Channel serverChannel;
	private String restAddress;

	private volatile boolean started;

	public RestServerEndpoint(RestServerEndpointConfiguration configuration) {
		Preconditions.checkNotNull(configuration);
		this.configuredAddress = configuration.getEndpointBindAddress();
		this.configuredPort = configuration.getEndpointBindPort();
		this.sslEngine = configuration.getSslEngine();

		this.restAddress = null;

		this.started = false;
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
	public void start() throws Exception {
		synchronized (lock) {
			if (started) {
				// RestServerEndpoint already started
				return;
			}

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

			log.info("Rest endpoint listening at {}" + ':' + "{}", address, port);

			final String protocol;

			if (sslEngine != null) {
				protocol = "https://";
			} else {
				protocol = "http://";
			}

			restAddress = protocol + address + ':' + port;

			restAddressFuture.complete(restAddress);

			started = true;
		}
	}

	/**
	 * Returns the address on which this endpoint is accepting requests.
	 *
	 * @return address on which this endpoint is accepting requests
	 */
	public InetSocketAddress getServerAddress() {
		Preconditions.checkState(started, "The RestServerEndpoint has not been started yet.");
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

	/**
	 * Returns the address of the REST server endpoint. Since the address is only known
	 * after the endpoint is started, it is returned as a future which is completed
	 * with the REST address at start up.
	 *
	 * @return REST address of this endpoint
	 */
	public String getRestAddress() {
		Preconditions.checkState(started, "The RestServerEndpoint has not been started yet.");
		return restAddress;
	}

	/**
	 * Stops this REST server endpoint.
	 */
	public void shutdown(Time timeout) {

		synchronized (lock) {
			if (!started) {
				// RestServerEndpoint has not been started
				return;
			}

			log.info("Shutting down rest endpoint.");

			CompletableFuture<?> channelFuture = new CompletableFuture<>();
			if (this.serverChannel != null) {
				this.serverChannel.close().addListener(ignored -> channelFuture.complete(null));
				serverChannel = null;
			}
			CompletableFuture<?> groupFuture = new CompletableFuture<>();
			CompletableFuture<?> childGroupFuture = new CompletableFuture<>();

			channelFuture.thenRun(() -> {
				if (bootstrap != null) {
					if (bootstrap.group() != null) {
						bootstrap.group().shutdownGracefully(0, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)
							.addListener(ignored -> groupFuture.complete(null));
					}
					if (bootstrap.childGroup() != null) {
						bootstrap.childGroup().shutdownGracefully(0, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)
							.addListener(ignored -> childGroupFuture.complete(null));
					}
					bootstrap = null;
				} else {
					// complete the group futures since there is nothing to stop
					groupFuture.complete(null);
					childGroupFuture.complete(null);
				}
			});

			try {
				CompletableFuture.allOf(groupFuture, childGroupFuture).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				log.info("Rest endpoint shutdown complete.");
			} catch (Exception e) {
				log.warn("Rest endpoint shutdown failed.", e);
			}

			restAddress = null;
			started = false;
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
}
