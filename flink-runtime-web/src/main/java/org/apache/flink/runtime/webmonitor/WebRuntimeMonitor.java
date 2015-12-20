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
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.Handler;
import io.netty.handler.codec.http.router.Router;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.handlers.ConstantTextHandler;
import org.apache.flink.runtime.webmonitor.handlers.ClusterOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobIdsHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobsOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.DashboardConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarAccessDeniedHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarListHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobCancellationHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobExceptionsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobManagerConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksTimesHandler;
import org.apache.flink.runtime.webmonitor.handlers.TaskManagersHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Promise;
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
	private final JobManagerRetriever retriever;

	private final Router router;

	private final ServerBootstrap bootstrap;

	private final Promise<String> jobManagerAddressPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

	private final FiniteDuration timeout;

	private Channel serverChannel;

	private final File webRootDir;

	private final File uploadDir;

	private AtomicBoolean isShutdown = new AtomicBoolean();


	public WebRuntimeMonitor(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem) throws IOException, InterruptedException {

		this.leaderRetrievalService = checkNotNull(leaderRetrievalService);

		final WebMonitorConfig cfg = new WebMonitorConfig(config);

		// create an empty directory in temp for the web server
		String fileName = String.format("flink-web-%s", UUID.randomUUID().toString());
		webRootDir = new File(System.getProperty("java.io.tmpdir"), fileName);

		// create storage for uploads
		fileName = String.format("flink-web-upload-%s", UUID.randomUUID().toString());
		uploadDir = new File(System.getProperty("java.io.tmpdir"), fileName);
		if (!uploadDir.mkdir() || !uploadDir.canWrite()) {
			throw new RuntimeException("Unable to create temporary directory to support jar uploads.");
		}

		LOG.info("Using directory {} for the web interface files", webRootDir);
		LOG.info("Using directory {} for the jar submissions", uploadDir);

		final WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(config);

		final boolean webSubmitAllow = config.getBoolean(
				ConfigConstants.JOB_MANAGER_WEB_SUBMISSION_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_SUBMISSION
		);

		// port configuration
		int configuredPort = cfg.getWebFrontendPort();
		if (configuredPort < 0) {
			throw new IllegalArgumentException("Web frontend port is invalid: " + configuredPort);
		}

		timeout = AkkaUtils.getTimeout(config);
		FiniteDuration lookupTimeout = AkkaUtils.getTimeout(config);

		retriever = new JobManagerRetriever(this, actorSystem, lookupTimeout, timeout);

		ExecutionGraphHolder currentGraphs = new ExecutionGraphHolder();

		router = new Router()
			// config how to interact with this web server
			.GET("/config", handler(new DashboardConfigHandler(cfg.getRefreshInterval())))

			// the overview - how many task managers, slots, free slots, ...
			.GET("/overview", handler(new ClusterOverviewHandler(DEFAULT_REQUEST_TIMEOUT)))

			// job manager configuration
			.GET("/jobmanager/config", handler(new JobManagerConfigHandler(config)))

			// overview over jobs
			.GET("/joboverview", handler(new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, true, true)))
			.GET("/joboverview/running", handler(new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, true, false)))
			.GET("/joboverview/completed", handler(new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, false, true)))

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

			.GET("/taskmanagers", handler(new TaskManagersHandler(DEFAULT_REQUEST_TIMEOUT)))
			.GET("/taskmanagers/:" + TaskManagersHandler.TASK_MANAGER_ID_KEY, handler(new TaskManagersHandler(DEFAULT_REQUEST_TIMEOUT)))

			// log and stdout
			.GET("/jobmanager/log", logFiles.logFile == null ? new ConstantTextHandler("(log file unavailable)") :
				new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, logFiles.logFile))

			.GET("/jobmanager/stdout", logFiles.stdOutFile == null ? new ConstantTextHandler("(stdout file unavailable)") :
				new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, logFiles.stdOutFile))

			// Cancel a job via GET (for proper integration with YARN this has to be performed via GET)
			.GET("/jobs/:jobid/yarn-cancel", handler(new JobCancellationHandler()))
			// DELETE is the preferred way of cancelling a job (Rest-conform)
			.DELETE("/jobs/:jobid", handler(new JobCancellationHandler()));

		if (webSubmitAllow) {
			router
				// fetch the list of uploaded jars.
				.GET("/jars", handler(new JarListHandler(uploadDir)))

				// get plan for an uploaded jar
				.GET("/jars/:jarid/plan", handler(new JarPlanHandler(uploadDir)))

				// run a jar
				.POST("/jars/:jarid/run", handler(new JarRunHandler(uploadDir, timeout)))

				// upload a jar
				.POST("/jars/upload", handler(new JarUploadHandler(uploadDir)))

				// delete an uploaded jar from submission interface
				.DELETE("/jars/:jarid", handler(new JarDeleteHandler(uploadDir)));
		} else {
			router
				// send an Access Denied message (sort of)
				// Every other GET request will go to the File Server, which will not provide
				// access to the jar directory anyway, because it doesn't exist in webRootDir.
				.GET("/jars", handler(new JarAccessDeniedHandler()));
		}

		// this handler serves all the static contents
		router.GET("/:*", new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, webRootDir));

		synchronized (startupShutdownLock) {

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
							.addLast(new HttpRequestHandler(uploadDir))
							.addLast(handler.name(), handler)
							.addLast(new PipelineErrorHandler());
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
	public void start(String jobManagerAkkaUrl) throws Exception {
		LOG.info("Starting with JobManager {} on port {}", jobManagerAkkaUrl, getServerPort());
		synchronized (startupShutdownLock) {
			jobManagerAddressPromise.success(jobManagerAkkaUrl);
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

		try {
			LOG.info("Removing web storage dir {}", uploadDir);
			FileUtils.deleteDirectory(uploadDir);
		} catch (Throwable t) {
			LOG.warn("Error while deleting web storage dir {}", uploadDir, t);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(handler, retriever, jobManagerAddressPromise.future(), timeout);
	}
}
