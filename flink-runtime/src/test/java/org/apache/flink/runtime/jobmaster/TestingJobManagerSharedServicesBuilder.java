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

import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.librarycache.ContextClassLoaderLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.restart.NoOrFixedIfCheckpointingEnabledRestartStrategyFactory;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureRequestCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.VoidBackPressureStatsTracker;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Builder for the {@link JobManagerSharedServices}.
 */
public class TestingJobManagerSharedServicesBuilder {

	private ScheduledExecutorService scheduledExecutorService;

	private LibraryCacheManager libraryCacheManager;

	private RestartStrategyFactory restartStrategyFactory;

	private BackPressureRequestCoordinator backPressureSampleCoordinator;

	private BackPressureStatsTracker backPressureStatsTracker;

	private BlobWriter blobWriter;

	public TestingJobManagerSharedServicesBuilder() {
		scheduledExecutorService = TestingUtils.defaultExecutor();
		libraryCacheManager = ContextClassLoaderLibraryCacheManager.INSTANCE;
		restartStrategyFactory = new NoOrFixedIfCheckpointingEnabledRestartStrategyFactory();
		backPressureSampleCoordinator = new BackPressureRequestCoordinator(Runnable::run, 10000);
		backPressureStatsTracker = VoidBackPressureStatsTracker.INSTANCE;
		blobWriter = VoidBlobWriter.getInstance();
	}

	public TestingJobManagerSharedServicesBuilder setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
		this.scheduledExecutorService = scheduledExecutorService;
		return this;
	}

	public TestingJobManagerSharedServicesBuilder setLibraryCacheManager(LibraryCacheManager libraryCacheManager) {
		this.libraryCacheManager = libraryCacheManager;
		return this;

	}

	public TestingJobManagerSharedServicesBuilder setRestartStrategyFactory(RestartStrategyFactory restartStrategyFactory) {
		this.restartStrategyFactory = restartStrategyFactory;
		return this;
	}

	public TestingJobManagerSharedServicesBuilder setBackPressureSampleCoordinator(BackPressureRequestCoordinator backPressureSampleCoordinator) {
		this.backPressureSampleCoordinator = backPressureSampleCoordinator;
		return this;
	}

	public TestingJobManagerSharedServicesBuilder setBackPressureStatsTracker(BackPressureStatsTracker backPressureStatsTracker) {
		this.backPressureStatsTracker = backPressureStatsTracker;
		return this;

	}

	public void setBlobWriter(BlobWriter blobWriter) {
		this.blobWriter = blobWriter;
	}

	public JobManagerSharedServices build() {
		return new JobManagerSharedServices(
			scheduledExecutorService,
			libraryCacheManager,
			restartStrategyFactory,
			backPressureSampleCoordinator,
			backPressureStatsTracker,
			blobWriter);
	}
}
