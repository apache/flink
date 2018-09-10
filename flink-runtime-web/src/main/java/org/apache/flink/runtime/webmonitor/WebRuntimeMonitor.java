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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.SSLEngineFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rest.handler.WebHandler;
import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointStatsCache;
import org.apache.flink.runtime.rest.handler.legacy.ClusterConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.ClusterOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.ConstantTextHandler;
import org.apache.flink.runtime.rest.handler.legacy.CurrentJobIdsHandler;
import org.apache.flink.runtime.rest.handler.legacy.DashboardConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.JobAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobCancellationHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobCancellationWithSavepointHandlers;
import org.apache.flink.runtime.rest.handler.legacy.JobConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobExceptionsHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobPlanHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobStoppingHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobVertexAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobVertexBackPressureHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobVertexTaskManagersHandler;
import org.apache.flink.runtime.rest.handler.legacy.JobsOverviewHandler;
import org.apache.flink.runtime.rest.handler.legacy.RequestHandler;
import org.apache.flink.runtime.rest.handler.legacy.SubtaskCurrentAttemptDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.SubtaskExecutionAttemptAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.legacy.SubtaskExecutionAttemptDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.SubtasksAllAccumulatorsHandler;
import org.apache.flink.runtime.rest.handler.legacy.SubtasksTimesHandler;
import org.apache.flink.runtime.rest.handler.legacy.TaskManagerLogHandler;
import org.apache.flink.runtime.rest.handler.legacy.TaskManagersHandler;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.StackTraceSampleCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.rest.handler.legacy.checkpoints.CheckpointStatsDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.checkpoints.CheckpointStatsDetailsSubtasksHandler;
import org.apache.flink.runtime.rest.handler.legacy.checkpoints.CheckpointStatsHandler;
import org.apache.flink.runtime.rest.handler.legacy.files.StaticFileServerHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.AggregatingJobsMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.AggregatingSubtasksMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.AggregatingTaskManagersMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.JobManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.JobMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.JobVertexMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.SubtaskMetricsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.TaskManagerMetricsHandler;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarAccessDeniedHandler;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarDeleteHandler;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarListHandler;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarPlanHandler;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarRunHandler;
import org.apache.flink.runtime.webmonitor.handlers.legacy.JarUploadHandler;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
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

	private final CompletableFuture<String> localRestAddress = new CompletableFuture<>();

	private final Time timeout;

	private final WebFrontendBootstrap netty;

	private final File webRootDir;

	private final File uploadDir;

	private final StackTraceSampleCoordinator stackTraceSamples;

	private final BackPressureStatsTrackerImpl backPressureStatsTrackerImpl;

	private final WebMonitorConfig cfg;

	private final ExecutionGraphCache executionGraphCache;

	private final ScheduledFuture<?> executionGraphCleanupTask;

	private AtomicBoolean cleanedUp = new AtomicBoolean();


	private MetricFetcher metricFetcher;

	public WebRuntimeMonitor(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			LeaderGatewayRetriever<JobManagerGateway> jobManagerRetriever,
			MetricQueryServiceRetriever queryServiceRetriever,
			Time timeout,
			ScheduledExecutor scheduledExecutor) throws IOException, InterruptedException {

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
			checkAndCreateUploadDir(uploadDir);
		}
		else {
			this.uploadDir = null;
		}

		final long timeToLive = cfg.getRefreshInterval() * 10L;

		this.executionGraphCache = new ExecutionGraphCache(
			timeout,
			Time.milliseconds(timeToLive));

		final long cleanupInterval = timeToLive * 2L;

		this.executionGraphCleanupTask = scheduledExecutor.scheduleWithFixedDelay(
			executionGraphCache::cleanup,
			cleanupInterval,
			cleanupInterval,
			TimeUnit.MILLISECONDS);

		// - Back pressure stats ----------------------------------------------

		stackTraceSamples = new StackTraceSampleCoordinator(scheduledExecutor, 60000);

		// Back pressure stats tracker config
		int cleanUpInterval = config.getInteger(WebOptions.BACKPRESSURE_CLEANUP_INTERVAL);

		int refreshInterval = config.getInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL);

		int numSamples = config.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES);

		int delay = config.getInteger(WebOptions.BACKPRESSURE_DELAY);

		Time delayBetweenSamples = Time.milliseconds(delay);

		backPressureStatsTrackerImpl = new BackPressureStatsTrackerImpl(
			stackTraceSamples,
			cleanUpInterval,
			numSamples,
			config.getInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL),
			delayBetweenSamples);

		// --------------------------------------------------------------------

		// Config to enable https access to the web-ui
		final SSLEngineFactory sslFactory;
		final boolean enableSSL = SSLUtils.isRestSSLEnabled(config) && config.getBoolean(WebOptions.SSL_ENABLED);
		if (enableSSL) {
			LOG.info("Enabling ssl for the web frontend");
			try {
				sslFactory = SSLUtils.createRestServerSSLEngineFactory(config);
			} catch (Exception e) {
				throw new IOException("Failed to initialize SSLContext for the web frontend", e);
			}
		} else {
			sslFactory = null;
		}
		metricFetcher = new MetricFetcher(retriever, queryServiceRetriever, scheduledExecutor, timeout);

		String defaultSavepointDir = config.getString(CheckpointingOptions.SAVEPOINT_DIRECTORY);

		JobCancellationWithSavepointHandlers cancelWithSavepoint = new JobCancellationWithSavepointHandlers(executionGraphCache, scheduledExecutor, defaultSavepointDir);
		RuntimeMonitorHandler triggerHandler = handler(cancelWithSavepoint.getTriggerHandler());
		RuntimeMonitorHandler inProgressHandler = handler(cancelWithSavepoint.getInProgressHandler());

		Router router = new Router();
		// config how to interact with this web server
		get(router, new DashboardConfigHandler(scheduledExecutor, cfg.getRefreshInterval()));

		// the overview - how many task managers, slots, free slots, ...
		get(router, new ClusterOverviewHandler(scheduledExecutor, DEFAULT_REQUEST_TIMEOUT));

		// job manager configuration
		get(router, new ClusterConfigHandler(scheduledExecutor, config));

		// metrics
		get(router, new JobManagerMetricsHandler(scheduledExecutor, metricFetcher));

		get(router, new AggregatingTaskManagersMetricsHandler(scheduledExecutor, metricFetcher));
		get(router, new TaskManagerMetricsHandler(scheduledExecutor, metricFetcher));

		// overview over jobs
		get(router, new JobsOverviewHandler(scheduledExecutor, DEFAULT_REQUEST_TIMEOUT));

		get(router, new CurrentJobIdsHandler(scheduledExecutor, DEFAULT_REQUEST_TIMEOUT));

		get(router, new JobDetailsHandler(executionGraphCache, scheduledExecutor, metricFetcher));

		get(router, new AggregatingJobsMetricsHandler(scheduledExecutor, metricFetcher));
		get(router, new JobMetricsHandler(scheduledExecutor, metricFetcher));

		get(router, new JobVertexMetricsHandler(scheduledExecutor, metricFetcher));

		get(router, new AggregatingSubtasksMetricsHandler(scheduledExecutor, metricFetcher));
		get(router, new SubtaskMetricsHandler(scheduledExecutor, metricFetcher));

		get(router, new JobVertexDetailsHandler(executionGraphCache, scheduledExecutor, metricFetcher));
		get(router, new SubtasksTimesHandler(executionGraphCache, scheduledExecutor));
		get(router, new JobVertexTaskManagersHandler(executionGraphCache, scheduledExecutor, metricFetcher));
		get(router, new JobVertexAccumulatorsHandler(executionGraphCache, scheduledExecutor));
		get(router, new JobVertexBackPressureHandler(executionGraphCache, scheduledExecutor, backPressureStatsTrackerImpl, refreshInterval));
		get(router, new SubtasksAllAccumulatorsHandler(executionGraphCache, scheduledExecutor));
		get(router, new SubtaskCurrentAttemptDetailsHandler(executionGraphCache, scheduledExecutor, metricFetcher));
		get(router, new SubtaskExecutionAttemptDetailsHandler(executionGraphCache, scheduledExecutor, metricFetcher));
		get(router, new SubtaskExecutionAttemptAccumulatorsHandler(executionGraphCache, scheduledExecutor));

		get(router, new JobPlanHandler(executionGraphCache, scheduledExecutor));
		get(router, new JobConfigHandler(executionGraphCache, scheduledExecutor));
		get(router, new JobExceptionsHandler(executionGraphCache, scheduledExecutor));
		get(router, new JobAccumulatorsHandler(executionGraphCache, scheduledExecutor));

		get(router, new TaskManagersHandler(scheduledExecutor, DEFAULT_REQUEST_TIMEOUT, metricFetcher));
		get(router,
			new TaskManagerLogHandler(
				retriever,
				scheduledExecutor,
				localRestAddress,
				timeout,
				TaskManagerLogHandler.FileMode.LOG,
				config));
		get(router,
			new TaskManagerLogHandler(
				retriever,
				scheduledExecutor,
				localRestAddress,
				timeout,
				TaskManagerLogHandler.FileMode.STDOUT,
				config));

		router
			// log and stdout
			.addGet("/jobmanager/log", logFiles.logFile == null ? new ConstantTextHandler("(log file unavailable)") :
				new StaticFileServerHandler<>(
					retriever,
					localRestAddress,
					timeout,
					logFiles.logFile))

			.addGet("/jobmanager/stdout", logFiles.stdOutFile == null ? new ConstantTextHandler("(stdout file unavailable)") :
				new StaticFileServerHandler<>(retriever, localRestAddress, timeout, logFiles.stdOutFile));

		// Cancel a job via GET (for proper integration with YARN this has to be performed via GET)
		get(router, new JobCancellationHandler(scheduledExecutor, timeout));
		// DELETE is the preferred way of canceling a job (Rest-conform)
		delete(router, new JobCancellationHandler(scheduledExecutor, timeout));

		get(router, triggerHandler);
		get(router, inProgressHandler);

		// stop a job via GET (for proper integration with YARN this has to be performed via GET)
		get(router, new JobStoppingHandler(scheduledExecutor, timeout));
		// DELETE is the preferred way of stopping a job (Rest-conform)
		delete(router, new JobStoppingHandler(scheduledExecutor, timeout));

		int maxCachedEntries = config.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);
		CheckpointStatsCache cache = new CheckpointStatsCache(maxCachedEntries);

		// Register the checkpoint stats handlers
		get(router, new CheckpointStatsHandler(executionGraphCache, scheduledExecutor));
		get(router, new CheckpointConfigHandler(executionGraphCache, scheduledExecutor));
		get(router, new CheckpointStatsDetailsHandler(executionGraphCache, scheduledExecutor, cache));
		get(router, new CheckpointStatsDetailsSubtasksHandler(executionGraphCache, scheduledExecutor, cache));

		if (webSubmitAllow) {
			// fetch the list of uploaded jars.
			get(router, new JarListHandler(scheduledExecutor, uploadDir));

			// get plan for an uploaded jar
			get(router, new JarPlanHandler(scheduledExecutor, uploadDir));

			// run a jar
			post(router, new JarRunHandler(scheduledExecutor, uploadDir, timeout, config));

			// upload a jar
			post(router, new JarUploadHandler(scheduledExecutor, uploadDir));

			// delete an uploaded jar from submission interface
			delete(router, new JarDeleteHandler(scheduledExecutor, uploadDir));
		} else {
			// send an Access Denied message
			JarAccessDeniedHandler jad = new JarAccessDeniedHandler(scheduledExecutor);
			get(router, jad);
			post(router, jad);
			delete(router, jad);
		}

		// this handler serves all the static contents
		router.addGet("/:*", new StaticFileServerHandler<>(
			retriever,
			localRestAddress,
			timeout,
			webRootDir));

		// add shutdown hook for deleting the directories and remaining temp files on shutdown
		ShutdownHookUtil.addShutdownHook(this::cleanup, getClass().getSimpleName(), LOG);

		this.netty = new WebFrontendBootstrap(router, LOG, uploadDir, sslFactory, configuredAddress, configuredPort, config);

		localRestAddress.complete(netty.getRestAddress());
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
			new JobsOverviewHandler.CurrentJobsOverviewJsonArchivist(),

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
	public void start() throws Exception {
		synchronized (startupShutdownLock) {
			leaderRetrievalService.start(retriever);

			long delay = backPressureStatsTrackerImpl.getCleanUpInterval();

			// Scheduled back pressure stats tracker cache cleanup. We schedule
			// this here repeatedly, because cache clean up only happens on
			// interactions with the cache. We need it to make sure that we
			// don't leak memory after completed jobs or long ago accessed stats.
			netty.getBootstrap().childGroup().scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					try {
						backPressureStatsTrackerImpl.cleanUpOperatorStatsCache();
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

			executionGraphCleanupTask.cancel(false);

			executionGraphCache.close();

			leaderRetrievalService.stop();

			netty.shutdown();

			stackTraceSamples.shutDown();

			backPressureStatsTrackerImpl.shutDown();

			cleanup();
		}
	}

	@Override
	public int getServerPort() {
		return netty.getServerPort();
	}

	@Override
	public String getRestAddress() {
		return netty.getRestAddress();
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
			router.addGet(path, handler);
		}
	}

	private void delete(Router router, RequestHandler handler) {
		delete(router, handler(handler));
	}

	private static <T extends ChannelInboundHandler & WebHandler> void delete(Router router, T handler) {
		for (String path : handler.getPaths()) {
			router.addDelete(path, handler);
		}
	}

	private void post(Router router, RequestHandler handler)  {
		post(router, handler(handler));
	}

	private static <T extends ChannelInboundHandler & WebHandler> void post(Router router, T handler) {
		for (String path : handler.getPaths()) {
			router.addPost(path, handler);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(cfg, handler, retriever, localRestAddress, timeout);
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

	public static void logExternalUploadDirDeletion(File uploadDir) {
		LOG.warn("Jar storage directory {} has been deleted externally. Previously uploaded jars are no longer available.", uploadDir.getAbsolutePath());
	}

	/**
	 * Checks whether the given directory exists and is writable. If it doesn't exist this method will attempt to create
	 * it.
	 *
	 * @param uploadDir directory to check
	 * @throws IOException if the directory does not exist and cannot be created, or if the directory isn't writable
	 */
	public static synchronized void checkAndCreateUploadDir(File uploadDir) throws IOException {
		if (uploadDir.exists() && uploadDir.canWrite()) {
			LOG.info("Using directory {} for web frontend JAR file uploads.", uploadDir);
		} else if (uploadDir.mkdirs() && uploadDir.canWrite()) {
			LOG.info("Created directory {} for web frontend JAR file uploads.", uploadDir);
		} else {
			LOG.warn("Jar upload directory {} cannot be created or is not writable.", uploadDir.getAbsolutePath());
			throw new IOException(
				String.format("Jar upload directory %s cannot be created or is not writable.",
					uploadDir.getAbsolutePath()));
		}
	}
}
