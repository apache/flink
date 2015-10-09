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

import akka.actor.ActorSystem;

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
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobManagerConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobExceptionsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobsOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.DashboardConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobIdsHandler;
import org.apache.flink.runtime.webmonitor.handlers.ClusterOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksTimesHandler;
import org.apache.flink.runtime.webmonitor.handlers.TaskManagersHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The root component of the web runtime monitor. This class starts the web server and creates
 * all request handlers for the REST API.
 * <p>
 * The web runtime monitor is based in Netty HTTP. It uses the Netty-Router library to route
 * HTTP requests of different paths to different response handlers. In addition, it serves the static
 * files of the web frontend, such as HTML, CSS, or JS files.
 */
public class WebRuntimeMonitor implements WebMonitor {

	/** By default, all requests to the JobManager have a timeout of 10 seconds */ 
	public static final FiniteDuration DEFAULT_REQUEST_TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);

	/** Logger for web frontend startup / shutdown messages */
	private static final Logger LOG = LoggerFactory.getLogger(WebRuntimeMonitor.class);

	// ------------------------------------------------------------------------

	/** Guarding concurrent modifications to the server channel pipeline during startup and shutdown */
	private final Object startupShutdownLock = new Object();

	private final LeaderRetrievalService leaderRetrievalService;

	/** LeaderRetrievalListener which stores the currently leading JobManager and its archive */
	private final JobManagerArchiveRetriever retriever;

	private final Router router;

	private final int configuredPort;

	private ServerBootstrap bootstrap;

	private Channel serverChannel;

	private final File webRootDir;

	private AtomicBoolean isShutdown = new AtomicBoolean();


	public WebRuntimeMonitor(
				Configuration config,
				LeaderRetrievalService leaderRetrievalService,
				ActorSystem actorSystem) throws IOException
	{
		this.leaderRetrievalService = checkNotNull(leaderRetrievalService);

		final WebMonitorConfig cfg = new WebMonitorConfig(config);

		// create an empty directory in temp for the web server
		String fileName = String.format("flink-web-%s", UUID.randomUUID().toString());
		webRootDir = new File(System.getProperty("java.io.tmpdir"), fileName);
		LOG.info("Using directory {} for the web interface files", webRootDir);

		// port configuration
		this.configuredPort = cfg.getWebFrontendPort();
		if (this.configuredPort < 0) {
			throw new IllegalArgumentException("Web frontend port is invalid: " + this.configuredPort);
		}

		FiniteDuration timeout = AkkaUtils.getTimeout(config);
		FiniteDuration lookupTimeout = AkkaUtils.getTimeout(config);

		retriever = new JobManagerArchiveRetriever(this, actorSystem, lookupTimeout, timeout);

		ExecutionGraphHolder currentGraphs = new ExecutionGraphHolder(retriever);

		router = new Router()
			// config how to interact with this web server
			.GET("/config", handler(new DashboardConfigHandler(cfg.getRefreshInterval())))

			// the overview - how many task managers, slots, free slots, ...
			.GET("/overview", handler(new ClusterOverviewHandler(retriever, DEFAULT_REQUEST_TIMEOUT)))

			// job manager configuration
			.GET("/jobmanager/config", handler(new JobManagerConfigHandler(config)))

			// overview over jobs
			.GET("/joboverview", handler(new CurrentJobsOverviewHandler(retriever, DEFAULT_REQUEST_TIMEOUT, true, true)))
			.GET("/joboverview/running", handler(new CurrentJobsOverviewHandler(retriever, DEFAULT_REQUEST_TIMEOUT, true, false)))
			.GET("/joboverview/completed", handler(new CurrentJobsOverviewHandler(retriever, DEFAULT_REQUEST_TIMEOUT, false, true)))

			.GET("/jobs", handler(new CurrentJobIdsHandler(retriever, DEFAULT_REQUEST_TIMEOUT)))

			.GET("/jobs/:jobid", handler(new JobDetailsHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices", handler(new JobDetailsHandler(currentGraphs)))

			.GET("/jobs/:jobid/vertices/:vertexid", handler(new JobVertexDetailsHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices/:vertexid/subtasktimes", handler(new SubtasksTimesHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices/:vertexid/accumulators", handler(new JobVertexAccumulatorsHandler(currentGraphs)))

			.GET("/jobs/:jobid/vertices/:vertexid/subtasks/accumulators", handler(new SubtasksAllAccumulatorsHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum", handler(new SubtaskCurrentAttemptDetailsHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt", handler(new SubtaskExecutionAttemptDetailsHandler(currentGraphs)))
			.GET("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt/accumulators", handler(new SubtaskExecutionAttemptAccumulatorsHandler(currentGraphs)))

			.GET("/jobs/:jobid/plan", handler(new JobPlanHandler(currentGraphs)))
			.GET("/jobs/:jobid/config", handler(new JobConfigHandler(currentGraphs)))
			.GET("/jobs/:jobid/exceptions", handler(new JobExceptionsHandler(currentGraphs)))
			.GET("/jobs/:jobid/accumulators", handler(new JobAccumulatorsHandler(currentGraphs)))

			.GET("/taskmanagers", handler(new TaskManagersHandler(retriever, DEFAULT_REQUEST_TIMEOUT)))
			.GET("/taskmanagers/:" + TaskManagersHandler.TASK_MANAGER_ID_KEY, handler(new TaskManagersHandler(retriever, DEFAULT_REQUEST_TIMEOUT)))

			// this handler serves all the static contents
			.GET("/:*", new StaticFileServerHandler(webRootDir));
	}

	@Override
	public void start() throws Exception {
		synchronized (startupShutdownLock) {
			if (this.bootstrap != null) {
				throw new IllegalStateException("The server has already been started");
			}

			// add shutdown hook for deleting the directory
			try {
				Runtime.getRuntime().addShutdownHook(new Thread() {
					@Override
					public void run() {
						shutdown();
					}
				});
			} catch (IllegalStateException e) {
				// race, JVM is in shutdown already, we can safely ignore this
				LOG.debug("Unable to add shutdown hook, shutdown already in progress", e);
			} catch(Throwable t) {
				// these errors usually happen when the shutdown is already in progress
				LOG.warn("Error while adding shutdown hook", t);
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

			leaderRetrievalService.start(retriever);
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (startupShutdownLock) {
			leaderRetrievalService.stop();

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

			shutdown();
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

	private void shutdown() {
		if (!isShutdown.compareAndSet(false, true)) {
			return;
		}
		try {
			LOG.info("Removing web root dir {}", webRootDir);
			FileUtils.deleteDirectory(webRootDir);
		} catch (Throwable t) {
			LOG.warn("Error while deleting web root dir {}", webRootDir, t);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(handler);
	}
}
