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

package org.pache.flink.metrics.tests;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobManagerMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagerMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.FlinkDistribution;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * End-to-end test for the availability of metrics.
 */
@Category(TravisGroup1.class)
public class MetricsAvailabilityITCase extends TestLogger {

	private static final String HOST = "localhost";
	private static final int PORT = 8081;

	@Rule
	public final FlinkDistribution dist = new FlinkDistribution();

	@Nullable
	private static ScheduledExecutorService scheduledExecutorService = null;

	@BeforeClass
	public static void startExecutor() {
		scheduledExecutorService = Executors.newScheduledThreadPool(4);
	}

	@AfterClass
	public static void shutdownExecutor() {
		if (scheduledExecutorService != null) {
			scheduledExecutorService.shutdown();
		}
	}

	@Test
	public void testReporter() throws Exception {
		dist.startFlinkCluster();

		final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), scheduledExecutorService);

		checkJobManagerMetricAvailability(restClient);

		final Collection<ResourceID> taskManagerIds = getTaskManagerIds(restClient);

		for (final ResourceID taskManagerId : taskManagerIds) {
			checkTaskManagerMetricAvailability(restClient, taskManagerId);
		}
	}

	private static void checkJobManagerMetricAvailability(final RestClient restClient) throws Exception {
		final JobManagerMetricsHeaders headers = JobManagerMetricsHeaders.getInstance();
		final JobManagerMetricsMessageParameters parameters = headers.getUnresolvedMessageParameters();
		parameters.metricsFilterParameter.resolve(Collections.singletonList("numRegisteredTaskManagers"));

		fetchMetric(() ->
				restClient.sendRequest(HOST, PORT, headers, parameters, EmptyRequestBody.getInstance()),
			getMetricNamePredicate("numRegisteredTaskManagers"));
	}

	private static Collection<ResourceID> getTaskManagerIds(final RestClient restClient) throws Exception {
		final TaskManagersHeaders headers = TaskManagersHeaders.getInstance();

		final TaskManagersInfo response = fetchMetric(() ->
				restClient.sendRequest(
					HOST,
					PORT,
					headers,
					EmptyMessageParameters.getInstance(),
					EmptyRequestBody.getInstance()),
			taskManagersInfo -> !taskManagersInfo.getTaskManagerInfos().isEmpty());

		return response.getTaskManagerInfos().stream()
			.map(TaskManagerInfo::getResourceId)
			.collect(Collectors.toList());
	}

	private static void checkTaskManagerMetricAvailability(final RestClient restClient, final ResourceID taskManagerId) throws Exception {
		final TaskManagerMetricsHeaders headers = TaskManagerMetricsHeaders.getInstance();
		final TaskManagerMetricsMessageParameters parameters = headers.getUnresolvedMessageParameters();
		parameters.taskManagerIdParameter.resolve(taskManagerId);
		parameters.metricsFilterParameter.resolve(Collections.singletonList("Status.Network.TotalMemorySegments"));

		fetchMetric(() ->
				restClient.sendRequest(HOST, PORT, headers, parameters, EmptyRequestBody.getInstance()),
			getMetricNamePredicate("Status.Network.TotalMemorySegments"));
	}

	private static <X> X fetchMetric(final SupplierWithException<CompletableFuture<X>, IOException> clientOperation, final Predicate<X> predicate) throws InterruptedException, ExecutionException, TimeoutException {
		final CompletableFuture<X> responseFuture = FutureUtils.retrySuccessfulWithDelay(() -> {
				try {
					return clientOperation.get();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			},
			Time.seconds(1),
			Deadline.fromNow(Duration.ofSeconds(5)),
			predicate,
			new ScheduledExecutorServiceAdapter(scheduledExecutorService));

		return responseFuture.get(30, TimeUnit.SECONDS);
	}

	private static Predicate<MetricCollectionResponseBody> getMetricNamePredicate(final String metricName) {
		return response -> response.getMetrics().stream().anyMatch(metric -> metric.getId().equals(metricName));
	}
}
