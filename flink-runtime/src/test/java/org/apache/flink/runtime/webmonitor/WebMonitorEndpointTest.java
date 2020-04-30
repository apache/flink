/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.NoOpTransientBlobService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link WebMonitorEndpoint}.
 */
public class WebMonitorEndpointTest extends TestLogger {

	@Test
	public void cleansUpExpiredExecutionGraphs() throws Exception {
		final Configuration configuration = new Configuration();
		configuration.setString(RestOptions.ADDRESS, "localhost");
		configuration.setLong(WebOptions.REFRESH_INTERVAL, 5L);
		final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		final long timeout = 10000L;

		final OneShotLatch cleanupLatch = new OneShotLatch();
		final TestingExecutionGraphCache executionGraphCache = TestingExecutionGraphCache.newBuilder()
			.setCleanupRunnable(cleanupLatch::trigger)
			.build();
		try (final WebMonitorEndpoint<RestfulGateway> webMonitorEndpoint = new WebMonitorEndpoint<>(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			CompletableFuture::new,
			configuration,
			RestHandlerConfiguration.fromConfiguration(configuration),
			CompletableFuture::new,
			NoOpTransientBlobService.INSTANCE,
			executor,
			VoidMetricFetcher.INSTANCE,
			new TestingLeaderElectionService(),
			executionGraphCache,
			new TestingFatalErrorHandler())) {

			webMonitorEndpoint.start();

			// check that the cleanup will be triggered
			cleanupLatch.await(timeout, TimeUnit.MILLISECONDS);
		} finally {
			ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
		}
	}
}
