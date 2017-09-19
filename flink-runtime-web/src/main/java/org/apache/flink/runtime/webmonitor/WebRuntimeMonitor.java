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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.BlobView;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler;
import org.apache.flink.runtime.webmonitor.handlers.ClusterOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.ConstantTextHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobIdsHandler;
import org.apache.flink.runtime.webmonitor.handlers.CurrentJobsOverviewHandler;
import org.apache.flink.runtime.webmonitor.handlers.DashboardConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarAccessDeniedHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarListHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobCancellationHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobCancellationWithSavepointHandlers;
import org.apache.flink.runtime.webmonitor.handlers.JobConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobExceptionsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobManagerConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobStoppingHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexBackPressureHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.JobVertexTaskManagersHandler;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtaskExecutionAttemptDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.webmonitor.handlers.SubtasksTimesHandler;
import org.apache.flink.runtime.webmonitor.handlers.TaskManagerLogHandler;
import org.apache.flink.runtime.webmonitor.handlers.TaskManagersHandler;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsDetailsHandler;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsDetailsSubtasksHandler;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsHandler;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.metrics.JobManagerMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.JobMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.JobVertexMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.TaskManagerMetricsHandler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The root component of the web runtime monitor. This class starts the web server and creates
 * all request handlers for the REST API.
 *
 * <p>The web runtime monitor is based in Netty HTTP. It uses the Netty-Router library to route
 * HTTP requests of different paths to different response handlers. In addition, it serves the static
 * files of the web frontend, such as HTML, CSS, or JS files.
 */
public class WebRuntimeMonitor implements WebMonitor {

	/** By default, all requests to the JobManager have a timeout of 10 seconds. */
	public static final Time DEFAULT_REQUEST_TIMEOUT = Time.seconds(10L);

	/** Logger for web frontend startup / shutdown messages. */
	private static final Logger LOG = LoggerFactory.getLogger(WebRuntimeMonitor.class);

	// ------------------------------------------------------------------------

	/** Guarding concurrent modifications to the server channel pipeline during startup and shutdown. */
	private final Object startupShutdownLock = new Object();

	private final LeaderRetrievalService leaderRetrievalService;

	/** Service which retrieves the currently leading JobManager and opens a JobManagerGateway. */
	private final LeaderGatewayRetriever<JobManagerGateway> retriever;

	private final SSLContext serverSSLContext;

	private final CompletableFuture<String> jobManagerAddressFuture = new CompletableFuture<>();

	private final Time timeout;

	private final WebFrontendBootstrap netty;

	private final File webRootDir;

	private final File uploadDir;

	private final StackTraceSampleCoordinator stackTraceSamples;

	private final BackPressureStatsTracker backPressureStatsTracker;

	private final WebMonitorConfig cfg;

	private AtomicBoolean cleanedUp = new AtomicBoolean();


	private MetricFetcher metricFetcher;

	public WebRuntimeMonitor(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			BlobView blobView,
			LeaderGatewayRetriever<JobManagerGateway> jobManagerRetriever,
			MetricQueryServiceRetriever queryServiceRetriever,
			Time timeout,
			Executor executor) throws IOException, InterruptedException {

		this.leaderRetrievalService = checkNotNull(leaderRetrievalService);
		this.retriever = Preconditions.checkNotNull(jobManagerRetriever);
		this.timeout = Preconditions.checkNotNull(timeout);

		this.cfg = new WebMonitorConfig(config);

		final String configuredAddress = cfg.getWebFrontendAddress();

		final int configuredPort = cfg.getWebFrontendPort();
		if (configuredPort < 0) {
			throw new IllegalArgumentException("Web frontend port is invalid: " + configuredPort);
		}

		final WebMonitorUtils.LogFileLocation logFiles = WebMonitorUtils.LogFileLocation.find(config);

		// create an empty directory in temp for the web server
		String rootDirFileName = "flink-web-" + UUID.randomUUID();
		webRootDir = new File(getBaseDir(config), rootDirFileName);
		LOG.info("Using directory {} for the web interface files", webRootDir);

		final boolean webSubmitAllow = cfg.isProgramSubmitEnabled();
		if (webSubmitAllow) {
			// create storage for uploads
			this.uploadDir = getUploadDir(config);
			// the upload directory should either 1. exist and writable or 2. can be created and writable
			if (!(uploadDir.exists() && uploadDir.canWrite()) && !(uploadDir.mkdirs() && uploadDir.canWrite())) {
				throw new IOException(
					String.format("Jar upload directory %s cannot be created or is not writable.",
						uploadDir.getAbsolutePath()));
			}
			LOG.info("Using directory {} for web frontend JAR file uploads", uploadDir);
		}
		else {
			this.uploadDir = null;
		}

		ExecutionGraphHolder currentGraphs = new ExecutionGraphHolder();

		// - Back pressure stats ----------------------------------------------

		stackTraceSamples = new StackTraceSampleCoordinator(executor, 60000);

		// Back pressure stats tracker config
		int cleanUpInterval = config.getInteger(WebOptions.BACKPRESSURE_CLEANUP_INTERVAL);

		int refreshInterval = config.getInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL);

		int numSamples = config.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES);

		int delay = config.getInteger(WebOptions.BACKPRESSURE_DELAY);

		Time delayBetweenSamples = Time.milliseconds(delay);

		backPressureStatsTracker = new BackPressureStatsTracker(
				stackTraceSamples, cleanUpInterval, numSamples, delayBetweenSamples);

		// --------------------------------------------------------------------

		// Config to enable https access to the web-ui
		boolean enableSSL = config.getBoolean(WebOptions.SSL_ENABLED) && SSLUtils.getSSLEnabled(config);

		if (enableSSL) {
			LOG.info("Enabling ssl for the web frontend");
			try {
				serverSSLContext = SSLUtils.createSSLServerContext(config);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the web frontend", e);
			}
		} else {
			serverSSLContext = null;
		}
		metricFetcher = new MetricFetcher(retriever, queryServiceRetriever, executor, timeout);

		String defaultSavepointDir = config.getString(CoreOptions.SAVEPOINT_DIRECTORY);

		JobCancellationWithSavepointHandlers cancelWithSavepoint = new JobCancellationWithSavepointHandlers(currentGraphs, executor, defaultSavepointDir);
		RuntimeMonitorHandler triggerHandler = handler(cancelWithSavepoint.getTriggerHandler());
		RuntimeMonitorHandler inProgressHandler = handler(cancelWithSavepoint.getInProgressHandler());

		Router router = new Router();
		// config how to interact with this web server
		get(router, new DashboardConfigHandler(executor, cfg.getRefreshInterval()));

		// the overview - how many task managers, slots, free slots, ...
		get(router, new ClusterOverviewHandler(executor, DEFAULT_REQUEST_TIMEOUT));

		// job manager configuration
		get(router, new JobManagerConfigHandler(executor, config));

		// overview over jobs
		get(router, new CurrentJobsOverviewHandler(executor, DEFAULT_REQUEST_TIMEOUT, true, true));
		get(router, new CurrentJobsOverviewHandler(executor, DEFAULT_REQUEST_TIMEOUT, true, false));
		get(router, new CurrentJobsOverviewHandler(executor, DEFAULT_REQUEST_TIMEOUT, false, true));

		get(router, new CurrentJobIdsHandler(executor, DEFAULT_REQUEST_TIMEOUT));

		get(router, new JobDetailsHandler(currentGraphs, executor, metricFetcher));

		get(router, new JobVertexDetailsHandler(currentGraphs, executor, metricFetcher));
		get(router, new SubtasksTimesHandler(currentGraphs, executor));
		get(router, new JobVertexTaskManagersHandler(currentGraphs, executor, metricFetcher));
		get(router, new JobVertexAccumulatorsHandler(currentGraphs, executor));
		get(router, new JobVertexBackPressureHandler(currentGraphs, executor,	backPressureStatsTracker, refreshInterval));
		get(router, new JobVertexMetricsHandler(executor, metricFetcher));
		get(router, new SubtasksAllAccumulatorsHandler(currentGraphs, executor));
		get(router, new SubtaskCurrentAttemptDetailsHandler(currentGraphs, executor, metricFetcher));
		get(router, new SubtaskExecutionAttemptDetailsHandler(currentGraphs, executor, metricFetcher));
		get(router, new SubtaskExecutionAttemptAccumulatorsHandler(currentGraphs, executor));

		get(router, new JobPlanHandler(currentGraphs, executor));
		get(router, new JobConfigHandler(currentGraphs, executor));
		get(router, new JobExceptionsHandler(currentGraphs, executor));
		get(router, new JobAccumulatorsHandler(currentGraphs, executor));
		get(router, new JobMetricsHandler(executor, metricFetcher));

		get(router, new TaskManagersHandler(executor, DEFAULT_REQUEST_TIMEOUT, metricFetcher));
		get(router,
			new TaskManagerLogHandler(
				retriever,
				executor,
				jobManagerAddressFuture,
				timeout,
				TaskManagerLogHandler.FileMode.LOG,
				config,
				enableSSL,
				blobView));
		get(router,
			new TaskManagerLogHandler(
				retriever,
				executor,
				jobManagerAddressFuture,
				timeout,
				TaskManagerLogHandler.FileMode.STDOUT,
				config,
				enableSSL,
				blobView));
		get(router, new TaskManagerMetricsHandler(executor, metricFetcher));

		router
			// log and stdout
			.GET("/jobmanager/log", logFiles.logFile == null ? new ConstantTextHandler("(log file unavailable)") :
				new StaticFileServerHandler<>(
					retriever,
					jobManagerAddressFuture,
					timeout,
					logFiles.logFile,
					enableSSL))

			.GET("/jobmanager/stdout", logFiles.stdOutFile == null ? new ConstantTextHandler("(stdout file unavailable)") :
				new StaticFileServerHandler<>(retriever, jobManagerAddressFuture, timeout, logFiles.stdOutFile,
					enableSSL));

		get(router, new JobManagerMetricsHandler(executor, metricFetcher));

		// Cancel a job via GET (for proper integration with YARN this has to be performed via GET)
		get(router, new JobCancellationHandler(executor, timeout));
		// DELETE is the preferred way of canceling a job (Rest-conform)
		delete(router, new JobCancellationHandler(executor, timeout));

		get(router, triggerHandler);
		get(router, inProgressHandler);

		// stop a job via GET (for proper integration with YARN this has to be performed via GET)
		get(router, new JobStoppingHandler(executor, timeout));
		// DELETE is the preferred way of stopping a job (Rest-conform)
		delete(router, new JobStoppingHandler(executor, timeout));

		int maxCachedEntries = config.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);
		CheckpointStatsCache cache = new CheckpointStatsCache(maxCachedEntries);

		// Register the checkpoint stats handlers
		get(router, new CheckpointStatsHandler(currentGraphs, executor));
		get(router, new CheckpointConfigHandler(currentGraphs, executor));
		get(router, new CheckpointStatsDetailsHandler(currentGraphs, executor, cache));
		get(router, new CheckpointStatsDetailsSubtasksHandler(currentGraphs, executor, cache));

		if (webSubmitAllow) {
			// fetch the list of uploaded jars.
			get(router, new JarListHandler(executor, uploadDir));

			// get plan for an uploaded jar
			get(router, new JarPlanHandler(executor, uploadDir));

			// run a jar
			post(router, new JarRunHandler(executor, uploadDir, timeout, config));

			// upload a jar
			post(router, new JarUploadHandler(executor, uploadDir));

			// delete an uploaded jar from submission interface
			delete(router, new JarDeleteHandler(executor, uploadDir));
		} else {
			// send an Access Denied message
			JarAccessDeniedHandler jad = new JarAccessDeniedHandler(executor);
			get(router, jad);
			post(router, jad);
			delete(router, jad);
		}

		// this handler serves all the static contents
		router.GET("/:*", new StaticFileServerHandler<>(
			retriever,
			jobManagerAddressFuture,
			timeout,
			webRootDir,
			enableSSL));

		// add shutdown hook for deleting the directories and remaining temp files on shutdown
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					cleanup();
				}
			});
		} catch (IllegalStateException e) {
			// race, JVM is in shutdown already, we can safely ignore this
			LOG.debug("Unable to add shutdown hook, shutdown already in progress", e);
		} catch (Throwable t) {
			// these errors usually happen when the shutdown is already in progress
			LOG.warn("Error while adding shutdown hook", t);
		}

		this.netty = new WebFrontendBootstrap(router, LOG, uploadDir, serverSSLContext, configuredAddress, configuredPort, config);
	}

	/**
	 * Returns an array of all {@link JsonArchivist}s that are relevant for the history server.
	 *
	 * <p>This method is static to allow easier access from the {@link MemoryArchivist}. Requiring a reference
	 * would imply that the WebRuntimeMonitor is always created before the archivist, which may not hold for all
	 * deployment modes.
	 *
	 <p>Similarly, no handler implements the JsonArchivist interface itself but instead contains a separate implementing
	 * class; otherwise we would either instantiate several handlers even though their main functionality isn't
	 * required, or yet again require that the WebRuntimeMonitor is started before the archivist.
	 *
	 * @return array of all JsonArchivists relevant for the history server
	 */
	public static JsonArchivist[] getJsonArchivists() {
		JsonArchivist[] archivists = {
			new CurrentJobsOverviewHandler.CurrentJobsOverviewJsonArchivist(),

			new JobPlanHandler.JobPlanJsonArchivist(),
			new JobConfigHandler.JobConfigJsonArchivist(),
			new JobExceptionsHandler.JobExceptionsJsonArchivist(),
			new JobDetailsHandler.JobDetailsJsonArchivist(),
			new JobAccumulatorsHandler.JobAccumulatorsJsonArchivist(),

			new CheckpointStatsHandler.CheckpointStatsJsonArchivist(),
			new CheckpointConfigHandler.CheckpointConfigJsonArchivist(),
			new CheckpointStatsDetailsHandler.CheckpointStatsDetailsJsonArchivist(),
			new CheckpointStatsDetailsSubtasksHandler.CheckpointStatsDetailsSubtasksJsonArchivist(),

			new JobVertexDetailsHandler.JobVertexDetailsJsonArchivist(),
			new SubtasksTimesHandler.SubtasksTimesJsonArchivist(),
			new JobVertexTaskManagersHandler.JobVertexTaskManagersJsonArchivist(),
			new JobVertexAccumulatorsHandler.JobVertexAccumulatorsJsonArchivist(),
			new SubtasksAllAccumulatorsHandler.SubtasksAllAccumulatorsJsonArchivist(),

			new SubtaskExecutionAttemptDetailsHandler.SubtaskExecutionAttemptDetailsJsonArchivist(),
			new SubtaskExecutionAttemptAccumulatorsHandler.SubtaskExecutionAttemptAccumulatorsJsonArchivist()
			};
		return archivists;
	}

	@Override
	public void start(String jobManagerAkkaUrl) throws Exception {
		LOG.info("Starting with JobManager {} on port {}", jobManagerAkkaUrl, getServerPort());

		synchronized (startupShutdownLock) {
			jobManagerAddressFuture.complete(jobManagerAkkaUrl);
			leaderRetrievalService.start(retriever);

			long delay = backPressureStatsTracker.getCleanUpInterval();

			// Scheduled back pressure stats tracker cache cleanup. We schedule
			// this here repeatedly, because cache clean up only happens on
			// interactions with the cache. We need it to make sure that we
			// don't leak memory after completed jobs or long ago accessed stats.
			netty.getBootstrap().childGroup().scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					try {
						backPressureStatsTracker.cleanUpOperatorStatsCache();
					} catch (Throwable t) {
						LOG.error("Error during back pressure stats cache cleanup.", t);
					}
				}
			}, delay, delay, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (startupShutdownLock) {
			leaderRetrievalService.stop();

			netty.shutdown();

			stackTraceSamples.shutDown();

			backPressureStatsTracker.shutDown();

			cleanup();
		}
	}

	@Override
	public int getServerPort() {
		return netty.getServerPort();
	}

	private void cleanup() {
		if (!cleanedUp.compareAndSet(false, true)) {
			return;
		}
		try {
			LOG.info("Removing web dashboard root cache directory {}", webRootDir);
			FileUtils.deleteDirectory(webRootDir);
		} catch (Throwable t) {
			LOG.warn("Error while deleting web root directory {}", webRootDir, t);
		}

		if (uploadDir != null) {
			try {
				LOG.info("Removing web dashboard jar upload directory {}", uploadDir);
				FileUtils.deleteDirectory(uploadDir);
			} catch (Throwable t) {
				LOG.warn("Error while deleting web storage dir {}", uploadDir, t);
			}
		}
	}

	/** These methods are used in the route path setup. They register the given {@link RequestHandler} or
	 * {@link RuntimeMonitorHandler} with the given {@link Router} for the respective REST method.
	 * The REST paths under which they are registered are defined by the handlers. **/

	private void get(Router router, RequestHandler handler) {
		get(router, handler(handler));
	}

	private static <T extends ChannelInboundHandler & WebHandler> void get(Router router, T handler) {
		for (String path : handler.getPaths()) {
			router.GET(path, handler);
		}
	}

	private void delete(Router router, RequestHandler handler) {
		delete(router, handler(handler));
	}

	private static <T extends ChannelInboundHandler & WebHandler> void delete(Router router, T handler) {
		for (String path : handler.getPaths()) {
			router.DELETE(path, handler);
		}
	}

	private void post(Router router, RequestHandler handler)  {
		post(router, handler(handler));
	}

	private static <T extends ChannelInboundHandler & WebHandler> void post(Router router, T handler) {
		for (String path : handler.getPaths()) {
			router.POST(path, handler);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(cfg, handler, retriever, jobManagerAddressFuture, timeout,
			serverSSLContext !=  null);
	}

	File getBaseDir(Configuration configuration) {
		return new File(getBaseDirStr(configuration));
	}

	private String getBaseDirStr(Configuration configuration) {
		return configuration.getString(WebOptions.TMP_DIR);
	}

	private File getUploadDir(Configuration configuration) {
		File baseDir = new File(configuration.getString(WebOptions.UPLOAD_DIR,
			getBaseDirStr(configuration)));

		boolean uploadDirSpecified = configuration.contains(WebOptions.UPLOAD_DIR);
		return uploadDirSpecified ? baseDir : new File(baseDir, "flink-web-" + UUID.randomUUID());
	}
}
