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

import io.netty.handler.codec.http.router.Router;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
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
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsHandler;
import org.apache.flink.runtime.webmonitor.handlers.checkpoints.CheckpointStatsDetailsSubtasksHandler;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.metrics.JobManagerMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.JobMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.JobVertexMetricsHandler;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.metrics.TaskManagerMetricsHandler;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.ExecutionContext$;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

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

	private final SSLContext serverSSLContext;

	private final Promise<String> jobManagerAddressPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

	private final FiniteDuration timeout;
	private final WebFrontendBootstrap netty;

	private final File webRootDir;

	private final File uploadDir;

	private final StackTraceSampleCoordinator stackTraceSamples;

	private final BackPressureStatsTracker backPressureStatsTracker;

	private AtomicBoolean cleanedUp = new AtomicBoolean();

	private ExecutorService executorService;

	private MetricFetcher metricFetcher;

	public WebRuntimeMonitor(
			Configuration config,
			LeaderRetrievalService leaderRetrievalService,
			ActorSystem actorSystem) throws IOException, InterruptedException {

		this.leaderRetrievalService = checkNotNull(leaderRetrievalService);
		this.timeout = AkkaUtils.getTimeout(config);
		this.retriever = new JobManagerRetriever(this, actorSystem, AkkaUtils.getTimeout(config), timeout);
		
		final WebMonitorConfig cfg = new WebMonitorConfig(config);

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
			if (!(uploadDir.exists() && uploadDir.canWrite()) && !(uploadDir.mkdir() && uploadDir.canWrite())) {
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

		stackTraceSamples = new StackTraceSampleCoordinator(actorSystem.dispatcher(), 60000);

		// Back pressure stats tracker config
		int cleanUpInterval = config.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL);

		int refreshInterval = config.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL);

		int numSamples = config.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES);

		int delay = config.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_BACK_PRESSURE_DELAY,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_DELAY);

		Time delayBetweenSamples = Time.milliseconds(delay);

		backPressureStatsTracker = new BackPressureStatsTracker(
				stackTraceSamples, cleanUpInterval, numSamples, delayBetweenSamples);

		// --------------------------------------------------------------------

		executorService = new ForkJoinPool();

		ExecutionContextExecutor context = ExecutionContext$.MODULE$.fromExecutor(executorService);

		// Config to enable https access to the web-ui
		boolean enableSSL = config.getBoolean(
				ConfigConstants.JOB_MANAGER_WEB_SSL_ENABLED,
				ConfigConstants.DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED) &&
			SSLUtils.getSSLEnabled(config);

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
		metricFetcher = new MetricFetcher(actorSystem, retriever, context);

		String defaultSavepointDir = config.getString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY, null);

		JobCancellationWithSavepointHandlers cancelWithSavepoint = new JobCancellationWithSavepointHandlers(currentGraphs, context, defaultSavepointDir);
		RuntimeMonitorHandler triggerHandler = handler(cancelWithSavepoint.getTriggerHandler());
		RuntimeMonitorHandler inProgressHandler = handler(cancelWithSavepoint.getInProgressHandler());

		Router router = new Router();
		// config how to interact with this web server
		GET(router, new DashboardConfigHandler(cfg.getRefreshInterval()));

		// the overview - how many task managers, slots, free slots, ...
		GET(router, new ClusterOverviewHandler(DEFAULT_REQUEST_TIMEOUT));

		// job manager configuration
		GET(router, new JobManagerConfigHandler(config));

		// overview over jobs
		GET(router, new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, true, true));
		GET(router, new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, true, false));
		GET(router, new CurrentJobsOverviewHandler(DEFAULT_REQUEST_TIMEOUT, false, true));

		GET(router, new CurrentJobIdsHandler(DEFAULT_REQUEST_TIMEOUT));

		GET(router, new JobDetailsHandler(currentGraphs, metricFetcher));

		GET(router, new JobVertexDetailsHandler(currentGraphs, metricFetcher));
		GET(router, new SubtasksTimesHandler(currentGraphs));
		GET(router, new JobVertexTaskManagersHandler(currentGraphs, metricFetcher));
		GET(router, new JobVertexAccumulatorsHandler(currentGraphs));
		GET(router, new JobVertexBackPressureHandler(currentGraphs,	backPressureStatsTracker, refreshInterval));
		GET(router, new JobVertexMetricsHandler(metricFetcher));
		GET(router, new SubtasksAllAccumulatorsHandler(currentGraphs));
		GET(router, new SubtaskCurrentAttemptDetailsHandler(currentGraphs, metricFetcher));
		GET(router, new SubtaskExecutionAttemptDetailsHandler(currentGraphs, metricFetcher));
		GET(router, new SubtaskExecutionAttemptAccumulatorsHandler(currentGraphs));

		GET(router, new JobPlanHandler(currentGraphs));
		GET(router, new JobConfigHandler(currentGraphs));
		GET(router, new JobExceptionsHandler(currentGraphs));
		GET(router, new JobAccumulatorsHandler(currentGraphs));
		GET(router, new JobMetricsHandler(metricFetcher));

		GET(router, new TaskManagersHandler(DEFAULT_REQUEST_TIMEOUT, metricFetcher));
		GET(router, new TaskManagerLogHandler(retriever, context, jobManagerAddressPromise.future(), timeout,
				TaskManagerLogHandler.FileMode.LOG, config, enableSSL));
		GET(router, new TaskManagerLogHandler(retriever, context, jobManagerAddressPromise.future(), timeout,
				TaskManagerLogHandler.FileMode.STDOUT, config, enableSSL));
		GET(router, new TaskManagerMetricsHandler(metricFetcher));

		router
			// log and stdout
			.GET("/jobmanager/log", logFiles.logFile == null ? new ConstantTextHandler("(log file unavailable)") :
				new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, logFiles.logFile,
					enableSSL))

			.GET("/jobmanager/stdout", logFiles.stdOutFile == null ? new ConstantTextHandler("(stdout file unavailable)") :
				new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, logFiles.stdOutFile,
					enableSSL));

		GET(router, new JobManagerMetricsHandler(metricFetcher));

		// Cancel a job via GET (for proper integration with YARN this has to be performed via GET)
		GET(router, new JobCancellationHandler());
		// DELETE is the preferred way of canceling a job (Rest-conform)
		DELETE(router, new JobCancellationHandler());

		GET(router, triggerHandler);
		GET(router, inProgressHandler);

		// stop a job via GET (for proper integration with YARN this has to be performed via GET)
		GET(router, new JobStoppingHandler());
		// DELETE is the preferred way of stopping a job (Rest-conform)
		DELETE(router, new JobStoppingHandler());

		int maxCachedEntries = config.getInteger(
				ConfigConstants.JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE,
			ConfigConstants.DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE);
		CheckpointStatsCache cache = new CheckpointStatsCache(maxCachedEntries);

		// Register the checkpoint stats handlers
		GET(router, new CheckpointStatsHandler(currentGraphs));
		GET(router, new CheckpointConfigHandler(currentGraphs));
		GET(router, new CheckpointStatsDetailsHandler(currentGraphs, cache));
		GET(router, new CheckpointStatsDetailsSubtasksHandler(currentGraphs, cache));

		if (webSubmitAllow) {
			// fetch the list of uploaded jars.
			GET(router, new JarListHandler(uploadDir));

			// get plan for an uploaded jar
			GET(router, new JarPlanHandler(uploadDir));

			// run a jar
			POST(router, new JarRunHandler(uploadDir, timeout, config));

			// upload a jar
			POST(router, new JarUploadHandler(uploadDir));

			// delete an uploaded jar from submission interface
			DELETE(router, new JarDeleteHandler(uploadDir));
		} else {
			// send an Access Denied message
			JarAccessDeniedHandler jad = new JarAccessDeniedHandler();
			GET(router, jad);
			POST(router, jad);
			DELETE(router, jad);
		}

		// this handler serves all the static contents
		router.GET("/:*", new StaticFileServerHandler(retriever, jobManagerAddressPromise.future(), timeout, webRootDir,
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
	 * This method is static to allow easier access from the {@link MemoryArchivist}. Requiring a reference
	 * would imply that the WebRuntimeMonitor is always created before the archivist, which may not hold for all
	 * deployment modes.
	 * 
	 * Similarly, no handler implements the JsonArchivist interface itself but instead contains a separate implementing
	 * class; otherwise we would either instantiate several handlers even though their main functionality isn't
	 * required, or yet again require that the WebRuntimeMonitor is started before the archivist.
	 * 
	 * @return array of all JsonArchivists relevant for the history server
	 */
	public static JsonArchivist[] getJsonArchivists() {
		JsonArchivist[] archivists = new JsonArchivist[]{
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
			jobManagerAddressPromise.success(jobManagerAkkaUrl);
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

			executorService.shutdownNow();

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
	 * {@link RuntimeMonitorHandlerBase} with the given {@link Router} for the respective REST method.
	 * The REST paths under which they are registered are defined by the handlers. **/

	private void GET(Router router, RequestHandler handler) {
		GET(router, handler(handler));
	}

	private void GET(Router router, RuntimeMonitorHandlerBase handler) {
		for (String path : handler.getPaths()) {
			router.GET(path, handler);
		}
	}

	private void DELETE(Router router, RequestHandler handler) {
		DELETE(router, handler(handler));
	}

	private void DELETE(Router router, RuntimeMonitorHandlerBase handler) {
		for (String path : handler.getPaths()) {
			router.DELETE(path, handler);
		}
	}

	private void POST(Router router, RequestHandler handler) {
		POST(router, handler(handler));
	}

	private void POST(Router router, RuntimeMonitorHandlerBase handler) {
		for (String path : handler.getPaths()) {
			router.POST(path, handler);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private RuntimeMonitorHandler handler(RequestHandler handler) {
		return new RuntimeMonitorHandler(handler, retriever, jobManagerAddressPromise.future(), timeout,
			serverSSLContext !=  null);
	}

	File getBaseDir(Configuration configuration) {
		return new File(getBaseDirStr(configuration));
	}

	private String getBaseDirStr(Configuration configuration) {
		return configuration.getString(ConfigConstants.JOB_MANAGER_WEB_TMPDIR_KEY, System.getProperty("java.io.tmpdir"));
	}

	private File getUploadDir(Configuration configuration) {
		File baseDir = new File(configuration.getString(ConfigConstants.JOB_MANAGER_WEB_UPLOAD_DIR_KEY,
			getBaseDirStr(configuration)));

		boolean uploadDirSpecified = configuration.containsKey(ConfigConstants.JOB_MANAGER_WEB_UPLOAD_DIR_KEY);
		return uploadDirSpecified ? baseDir : new File(baseDir, "flink-web-" + UUID.randomUUID());
	}
}
