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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Router;

import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.handlers.ExecutionPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobSummaryHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVerticesOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobsOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestJobIdsHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestOverviewHandler;
import org.apache.flink.runtime.webmonitor.legacy.JobManagerInfoHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * The root component of the web runtime monitor.
 * 
 * <p>The web runtime monitor is based in Netty HTTP. It uses the Netty-Router library to route
 * HTTP requests of different paths to different response handlers. In addition, it serves the static
 * files of the web frontend, such as HTML, CSS, or JS files.</p>
 */
public class WebRuntimeMonitor implements WebMonitor {

	public static final FiniteDuration DEFAULT_REQUEST_TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);
	
	/** Logger for web frontend startup / shutdown messages */
	private static final Logger LOG = LoggerFactory.getLogger(WebRuntimeMonitor.class);
	
	/** Teh default path under which the static contents is stored */
	private static final String STATIC_CONTENTS_PATH = "resources/web-runtime-monitor";
	
	// ------------------------------------------------------------------------
	
	private final Object startupShutdownLock = new Object();
	
	private final Router router;

	private final int configuredPort;

	private ServerBootstrap bootstrap;
	
	private Channel serverChannel;

	
	public WebRuntimeMonitor(Configuration config, ActorGateway jobManager, ActorGateway archive) throws IOException {
		
		final WebMonitorConfig cfg = new WebMonitorConfig(config);
		
		// figure out where our static contents is
		final String flinkRoot = config.getString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, null);
		final String configuredWebRoot = cfg.getWebRoot();
		
		final File webRootDir;
		if (configuredWebRoot != null) {
			webRootDir = new File(configuredWebRoot);
		}
		else if (flinkRoot != null) {
			webRootDir = new File(flinkRoot, STATIC_CONTENTS_PATH);
		}
		else {
			throw new IllegalConfigurationException("The given configuration provides neither the web-document root (" 
					+ WebMonitorConfig.JOB_MANAGER_WEB_DOC_ROOT_KEY + "), not the Flink installation root ("
					+ ConfigConstants.FLINK_BASE_DIR_PATH_KEY + ").");
		}
		
		// validate that the doc root is a valid directory
		if (!(webRootDir.exists() && webRootDir.isDirectory() && webRootDir.canRead())) {
			throw new IllegalConfigurationException("The path to the static contents (" + 
					webRootDir.getAbsolutePath() + ") is not a readable directory.");
		}
		
		// port configuration
		this.configuredPort = cfg.getWebFrontendPort();
		if (this.configuredPort < 0) {
			throw new IllegalArgumentException("Web frontend port is invalid: " + this.configuredPort);
		}
		
		ExecutionGraphHolder currentGraphs = new ExecutionGraphHolder(jobManager);
		
		router = new Router()
			// config how to interact with this web server
			.GET("/config", handler(new RequestConfigHandler(cfg.getRefreshInterval())))
			
			// the overview - how many task managers, slots, free slots, ...
			.GET("/overview", handler(new RequestOverviewHandler(jobManager, DEFAULT_REQUEST_TIMEOUT)))

			// list of job ids for all jobs in each status
			.GET("/jobids", handler(new RequestJobIdsHandler(jobManager, DEFAULT_REQUEST_TIMEOUT)))

			// overview over jobs
			.GET("/joboverview", handler(new JobsOverviewHandler(jobManager, DEFAULT_REQUEST_TIMEOUT, true, true)))
			.GET("/joboverview/running",handler(new JobsOverviewHandler(jobManager, DEFAULT_REQUEST_TIMEOUT, true, false)))
			.GET("/joboverview/completed", handler(new JobsOverviewHandler(jobManager, DEFAULT_REQUEST_TIMEOUT, false, true)))

			.GET("/jobs/:jobid", handler(new JobSummaryHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices", handler(new JobVerticesOverviewHandler(currentGraphs)))
			.GET("/jobs/:jobid/plan", handler(new ExecutionPlanHandler(currentGraphs)))
			.GET("/jobs/:jobid/config", handler(new JobConfigHandler(currentGraphs)))

//			.GET("/running/:jobid/:jobvertex", handler(new ExecutionPlanHandler(currentGraphs)))

			// the handler for the legacy requests
			.GET("/jobsInfo", new JobManagerInfoHandler(jobManager, archive, DEFAULT_REQUEST_TIMEOUT))
					
			// this handler serves all the static contents
			.GET("/:*", new StaticFileServerHandler(webRootDir));
	}

	@Override
	public void start() throws Exception {
		synchronized (startupShutdownLock) {
			if (this.bootstrap != null) {
				throw new IllegalStateException("The server has already been started");
			}
			
			ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
	
				@Override
				protected void initChannel(SocketChannel ch) {
					Handler handler = new Handler(router);
					
					ch.pipeline()
						.addLast(new HttpServerCodec())
						.addLast(new HttpObjectAggregator(65536))
						.addLast(new ChunkedWriteHandler())
						.addLast(handler.name(), handler);
				}
			};
			
			NioEventLoopGroup bossGroup   = new NioEventLoopGroup(1);
			NioEventLoopGroup workerGroup = new NioEventLoopGroup();
	
			this.bootstrap = new ServerBootstrap();
			this.bootstrap
					.group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class)
					.childHandler(initializer);
	
			Channel ch = this.bootstrap.bind(configuredPort).sync().channel();
			this.serverChannel = ch;
			
			InetSocketAddress bindAddress = (InetSocketAddress) ch.localAddress();
			String address = bindAddress.getAddress().getHostAddress();
			int port = bindAddress.getPort();
			
			LOG.info("Web frontend listening at " + address + ':' + port);
		}
	}
	
	@Override
	public void stop() throws Exception {
		synchronized (startupShutdownLock) {
			if (this.serverChannel != null) {
				this.serverChannel.close().awaitUninterruptibly();
				this.serverChannel = null;
			}
			if (bootstrap != null) {
				if (bootstrap.group() != null) {
					bootstrap.group().shutdownGracefully();
				}
				this.bootstrap = null;
			}
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
