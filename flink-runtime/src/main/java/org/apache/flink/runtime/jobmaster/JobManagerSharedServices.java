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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTrackerImpl;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.StackTraceSampleCoordinator;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.ExceptionUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class which holds all auxiliary shared services used by the {@link JobMaster}.
 * Consequently, the {@link JobMaster} should never shut these services down.
 */
public class JobManagerSharedServices {

	private final ScheduledExecutorService scheduledExecutorService;

	private final LibraryCacheManager libraryCacheManager;

	private final RestartStrategyFactory restartStrategyFactory;

	private final StackTraceSampleCoordinator stackTraceSampleCoordinator;

	private final BackPressureStatsTracker backPressureStatsTracker;

	public JobManagerSharedServices(
			ScheduledExecutorService scheduledExecutorService,
			LibraryCacheManager libraryCacheManager,
			RestartStrategyFactory restartStrategyFactory,
			StackTraceSampleCoordinator stackTraceSampleCoordinator,
			BackPressureStatsTracker backPressureStatsTracker) {

		this.scheduledExecutorService = checkNotNull(scheduledExecutorService);
		this.libraryCacheManager = checkNotNull(libraryCacheManager);
		this.restartStrategyFactory = checkNotNull(restartStrategyFactory);
		this.stackTraceSampleCoordinator = checkNotNull(stackTraceSampleCoordinator);
		this.backPressureStatsTracker = checkNotNull(backPressureStatsTracker);
	}

	public ScheduledExecutorService getScheduledExecutorService() {
		return scheduledExecutorService;
	}

	public LibraryCacheManager getLibraryCacheManager() {
		return libraryCacheManager;
	}

	public RestartStrategyFactory getRestartStrategyFactory() {
		return restartStrategyFactory;
	}

	public BackPressureStatsTracker getBackPressureStatsTracker() {
		return backPressureStatsTracker;
	}

	/**
	 * Shutdown the {@link JobMaster} services.
	 *
	 * <p>This method makes sure all services are closed or shut down, even when an exception occurred
	 * in the shutdown of one component. The first encountered exception is thrown, with successive
	 * exceptions added as suppressed exceptions.
	 *
	 * @throws Exception The first Exception encountered during shutdown.
	 */
	public void shutdown() throws Exception {
		Throwable firstException = null;

		try {
			scheduledExecutorService.shutdownNow();
		} catch (Throwable t) {
			firstException = t;
		}

		libraryCacheManager.shutdown();
		stackTraceSampleCoordinator.shutDown();
		backPressureStatsTracker.shutDown();

		if (firstException != null) {
			ExceptionUtils.rethrowException(firstException, "Error while shutting down JobManager services");
		}
	}

	// ------------------------------------------------------------------------
	//  Creating the components from a configuration
	// ------------------------------------------------------------------------

	public static JobManagerSharedServices fromConfiguration(
			Configuration config,
			BlobServer blobServer) throws Exception {

		checkNotNull(config);
		checkNotNull(blobServer);

		final String classLoaderResolveOrder =
			config.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

		final String alwaysParentFirstLoaderString =
			config.getString(CoreOptions.ALWAYS_PARENT_FIRST_LOADER);
		final String[] alwaysParentFirstLoaderPatterns = alwaysParentFirstLoaderString.split(";");

		final BlobLibraryCacheManager libraryCacheManager =
			new BlobLibraryCacheManager(
				blobServer,
				FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder),
				alwaysParentFirstLoaderPatterns);

		final FiniteDuration timeout;
		try {
			timeout = AkkaUtils.getTimeout(config);
		} catch (NumberFormatException e) {
			throw new IllegalConfigurationException(AkkaUtils.formatDurationParsingErrorMessage());
		}

		final ScheduledExecutorService futureExecutor = Executors.newScheduledThreadPool(
				Hardware.getNumberCPUCores(),
				new ExecutorThreadFactory("jobmanager-future"));

		final StackTraceSampleCoordinator stackTraceSampleCoordinator =
			new StackTraceSampleCoordinator(futureExecutor, timeout.toMillis());
		final int cleanUpInterval = config.getInteger(WebOptions.BACKPRESSURE_CLEANUP_INTERVAL);
		final BackPressureStatsTrackerImpl backPressureStatsTracker = new BackPressureStatsTrackerImpl(
			stackTraceSampleCoordinator,
			cleanUpInterval,
			config.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
			config.getInteger(WebOptions.BACKPRESSURE_REFRESH_INTERVAL),
			Time.milliseconds(config.getInteger(WebOptions.BACKPRESSURE_DELAY)));

		futureExecutor.scheduleWithFixedDelay(
			backPressureStatsTracker::cleanUpOperatorStatsCache,
			cleanUpInterval,
			cleanUpInterval,
			TimeUnit.MILLISECONDS);

		return new JobManagerSharedServices(
			futureExecutor,
			libraryCacheManager,
			RestartStrategyFactory.createRestartStrategyFactory(config),
			stackTraceSampleCoordinator,
			backPressureStatsTracker);
	}
}
