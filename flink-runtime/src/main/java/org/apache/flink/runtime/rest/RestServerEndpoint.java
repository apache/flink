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

import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.handler.RouterHandler;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Handler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * An abstract class for netty-based REST server endpoints.
 */
public abstract class RestServerEndpoint {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final String configuredAddress;
	private final int configuredPort;
	private final SSLEngine sslEngine;
	private final Router router = new Router();

	private ServerBootstrap bootstrap;
	private Channel serverChannel;

	public RestServerEndpoint(RestServerEndpointConfiguration configuration) {
		this.configuredAddress = configuration.getEndpointBindAddress();
		this.configuredPort = configuration.getEndpointBindPort();
		this.sslEngine = configuration.getSslEngine();
	}

	/**
	 * This method is called at the beginning of {@link #start()} to setup all handlers that the REST server endpoint
	 * implementation requires.
	 */
	protected abstract Collection<AbstractRestHandler<?, ?>> initializeHandlers();

	/**
	 * Starts this REST server endpoint.
	 */
	public void start() {
		log.info("Starting rest endpoint.");
		initializeHandlers()
			.forEach(this::registerHandler);

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
					.addLast(new HttpObjectAggregator(1024 * 1024 * 10))
					.addLast(handler.name(), handler)
					.addLast(new PipelineErrorHandler(log));
			}
		};

		NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();

		bootstrap = new ServerBootstrap();
		bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(initializer);

		ChannelFuture ch;
		if (configuredAddress == null) {
			ch = bootstrap.bind(configuredPort);
		} else {
			ch = bootstrap.bind(configuredAddress, configuredPort);
		}
		serverChannel = ch.syncUninterruptibly().channel();

		InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
		String address = bindAddress.getAddress().getHostAddress();
		int port = bindAddress.getPort();

		log.info("Rest endpoint listening at {}" + ':' + "{}", address, port);
	}

	private <R extends RequestBody, P extends ResponseBody> void registerHandler(AbstractRestHandler<R, P> handler) {
		switch (handler.getMessageHeaders().getHttpMethod()) {
			case GET:
				router.GET(handler.getMessageHeaders().getTargetRestEndpointURL(), handler);
				break;
			case POST:
				router.POST(handler.getMessageHeaders().getTargetRestEndpointURL(), handler);
				break;
		}
	}

	/**
	 * Returns the address on which this endpoint is accepting requests.
	 *
	 * @return address on which this endpoint is accepting requests
	 */
	public InetSocketAddress getServerAddress() {
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
	 * Stops this REST server endpoint.
	 */
	public void shutdown() {
		log.info("Shutting down rest endpoint.");
		if (this.serverChannel != null) {
			this.serverChannel.close().awaitUninterruptibly();
		}
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			if (bootstrap.childGroup() != null) {
				bootstrap.childGroup().shutdownGracefully();
			}
		}
		log.info("Rest endpoint shutdown complete.");
	}
}
