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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.StackTraceSampleCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

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
		final SSLHandlerFactory sslFactory;
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

		Router router = new Router();

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
		JsonArchivist[] archivists = {};
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

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

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
