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
package org.apache.flink.runtime.webmonitor.utils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Router;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.webmonitor.HttpRequestHandler;
import org.apache.flink.runtime.webmonitor.PipelineErrorHandler;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.File;
import java.net.InetSocketAddress;

/**
 * This classes encapsulates the boot-strapping of netty for the web-frontend.
 */
public class WebFrontendBootstrap {
	private final Router router;
	private final Logger log;
	private final File uploadDir;
	private final SSLContext serverSSLContext;
	private final ServerBootstrap bootstrap;
	private final Channel serverChannel;

	public WebFrontendBootstrap(
			Router router,
			Logger log,
			File directory,
			SSLContext sslContext,
			String configuredAddress,
			int configuredPort,
			final Configuration config) throws InterruptedException {
		this.router = Preconditions.checkNotNull(router);
		this.log = Preconditions.checkNotNull(log);
		this.uploadDir = Preconditions.checkNotNull(directory);
		this.serverSSLContext = sslContext;

		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) {
				Handler handler = new Handler(WebFrontendBootstrap.this.router);

				// SSL should be the first handler in the pipeline
				if (serverSSLContext != null) {
					SSLEngine sslEngine = serverSSLContext.createSSLEngine();
					SSLUtils.setSSLVerAndCipherSuites(sslEngine, config);
					sslEngine.setUseClientMode(false);
					ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				ch.pipeline()
					.addLast(new HttpServerCodec())
					.addLast(new ChunkedWriteHandler())
					.addLast(new HttpRequestHandler(uploadDir))
					.addLast(handler.name(), handler)
					.addLast(new PipelineErrorHandler(WebFrontendBootstrap.this.log));
			}
		};

		NioEventLoopGroup bossGroup   = new NioEventLoopGroup(1);
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();

		this.bootstrap = new ServerBootstrap();
		this.bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(initializer);

		ChannelFuture ch;
		if (configuredAddress == null) {
			ch = this.bootstrap.bind(configuredPort);
		} else {
			ch = this.bootstrap.bind(configuredAddress, configuredPort);
		}
		this.serverChannel = ch.sync().channel();

		InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
		String address = bindAddress.getAddress().getHostAddress();
		int port = bindAddress.getPort();

		this.log.info("Web frontend listening at {}" + ':' + "{}", address, port);
	}

	public ServerBootstrap getBootstrap() {
		return bootstrap;
	}
	
	public int getServerPort() {
		Channel server = this.serverChannel;
		if (server != null) {
			try {
				return ((InetSocketAddress) server.localAddress()).getPort();
			}
			catch (Exception e) {
				log.error("Cannot access local server port", e);
			}
		}

		return -1;
	}
	
	public void shutdown() {
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
	}
}
