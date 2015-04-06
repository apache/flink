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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorRef;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.BadClientSilencer;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Router;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.webmonitor.handlers.ExecutionPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobSummaryHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVerticesOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestJobIdsHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestOverviewHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class WebRuntimeMonitor implements WebMonitor {

	public static final FiniteDuration DEFAULT_REQUEST_TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);
	
	public static final long DEFAULT_REFRESH_INTERVAL = 2000;
	
	
	private static final Logger LOG = LoggerFactory.getLogger(WebRuntimeMonitor.class);
	
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	
	private final Router router;

	private final int configuredPort;

	private ServerBootstrap bootstrap;
	
	private Channel serverChannel;

	
	public WebRuntimeMonitor(Configuration config, ActorRef jobManager, ActorRef archive) throws IOException {
		
		this.configuredPort = config.getInteger(ConfigConstants.JOB_MANAGER_NEW_WEB_PORT_KEY,
												ConfigConstants.DEFAULT_JOB_MANAGER_NEW_WEB_FRONTEND_PORT);
		if (this.configuredPort < 0) {
			throw new IllegalArgumentException("Web frontend port is " + this.configuredPort);
		}
		
		ExecutionGraphHolder currentGraphs = new ExecutionGraphHolder(jobManager);
		
		router = new Router()
			// config how to interact with this web server
			.GET("/config", handler(new RequestConfigHandler(DEFAULT_REFRESH_INTERVAL)))
			
			// the overview - how many task managers, slots, free slots, ...
			.GET("/overview", handler(new RequestOverviewHandler(jobManager)))

			// currently running jobs
			.GET("/jobs", handler(new RequestJobIdsHandler(jobManager)))
			.GET("/jobs/:jobid", handler(new JobSummaryHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices", handler(new JobVerticesOverviewHandler(currentGraphs)))
			.GET("/jobs/:jobid/plan", handler(new ExecutionPlanHandler(currentGraphs)));
//			.GET("/running/:jobid/:jobvertex", handler(new ExecutionPlanHandler(currentGraphs)))

	}

	@Override
	public void start() throws Exception {

		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) {
				Handler handler = new Handler(router);
				
				ch.pipeline()
					.addLast(new HttpServerCodec())
					.addLast(handler.name(), handler)
					.addLast(new BadClientSilencer());
			}
		};
		
		NioEventLoopGroup bossGroup   = new NioEventLoopGroup(1);
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();

		this.bootstrap = new ServerBootstrap();
		this.bootstrap.group(bossGroup, workerGroup)
				.childOption(ChannelOption.TCP_NODELAY,  java.lang.Boolean.TRUE)
				.childOption(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)
				.channel(NioServerSocketChannel.class)
				.childHandler(initializer);

		Channel ch = this.bootstrap.bind(configuredPort).sync().channel();
		
		InetSocketAddress bindAddress = (InetSocketAddress) ch.localAddress();
		String address = bindAddress.getAddress().getHostAddress();
		int port = bindAddress.getPort();
		
		LOG.info("Web frontend listening at " + address + ':' + port);
	}
	
	@Override
	public void stop() throws Exception {
		Channel server = this.serverChannel;
		ServerBootstrap bootstrap = this.bootstrap;
		
		if (server != null) {
			server.close().awaitUninterruptibly();
			this.serverChannel = null;
		}

		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			this.bootstrap = null;
		}
	}
	
	@Override
	public int getServerPort() {
		Channel server = this.serverChannel;
		if (server != null) {
			try {
				return ((InetSocketAddress) server.localAddress()).getPort();
			}
			catch (Exception e) {
				LOG.error("Cannot access local server port", e);
			}
		}
			
		return -1;
	}
	
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	private static RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(handler);
	}
}
