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

package org.apache.flink.runtime.webmonitor.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.webmonitor.retriever.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.dump.MetricDumpSerialization.MetricDumpDeserializer;

/**
 * The MetricFetcher can be used to fetch metrics from the JobManager and all registered TaskManagers.
 *
 * <p>Metrics will only be fetched when {@link MetricFetcher#update()} is called, provided that a sufficient time since
 * the last call has passed.
 */
public class MetricFetcher {
	private static final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);

	private final JobManagerRetriever retriever;
	private final MetricQueryServiceRetriever queryServiceRetriever;
	private final Executor executor;
	private final Time timeout;

	private final MetricStore metrics = new MetricStore();
	private final MetricDumpDeserializer deserializer = new MetricDumpDeserializer();

	private long lastUpdateTime;

	public MetricFetcher(
			JobManagerRetriever retriever,
			MetricQueryServiceRetriever queryServiceRetriever,
			Executor executor,
			Time timeout) {
		this.retriever = Preconditions.checkNotNull(retriever);
		this.queryServiceRetriever = Preconditions.checkNotNull(queryServiceRetriever);
		this.executor = Preconditions.checkNotNull(executor);
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	/**
	 * Returns the MetricStore containing all stored metrics.
	 *
	 * @return MetricStore containing all stored metrics;
	 */
	public MetricStore getMetricStore() {
		return metrics;
	}

	/**
	 * This method can be used to signal this MetricFetcher that the metrics are still in use and should be updated.
	 */
	public void update() {
		synchronized (this) {
			long currentTime = System.currentTimeMillis();
			if (currentTime - lastUpdateTime > 10000) { // 10 seconds have passed since the last update
				lastUpdateTime = currentTime;
				fetchMetrics();
			}
		}
	}

	private void fetchMetrics() {
		try {
			Optional<JobManagerGateway> optJobManagerGateway = retriever.getJobManagerGatewayNow();
			if (optJobManagerGateway.isPresent()) {
				final JobManagerGateway jobManagerGateway = optJobManagerGateway.get();

				/**
				 * Remove all metrics that belong to a job that is not running and no longer archived.
				 */
				CompletableFuture<MultipleJobsDetails> jobDetailsFuture = jobManagerGateway.requestJobDetails(true, true, timeout);

				jobDetailsFuture.whenCompleteAsync(
					(MultipleJobsDetails jobDetails, Throwable throwable) -> {
						if (throwable != null) {
							LOG.debug("Fetching of JobDetails failed.", throwable);
						} else {
							ArrayList<String> toRetain = new ArrayList<>();
							for (JobDetails job : jobDetails.getRunningJobs()) {
								toRetain.add(job.getJobId().toString());
							}
							for (JobDetails job : jobDetails.getFinishedJobs()) {
								toRetain.add(job.getJobId().toString());
							}
							synchronized (metrics) {
								metrics.jobs.keySet().retainAll(toRetain);
							}
						}
					},
					executor);

				String jobManagerPath = jobManagerGateway.getAddress();
				String jmQueryServicePath = jobManagerPath.substring(0, jobManagerPath.lastIndexOf('/') + 1) + MetricQueryService.METRIC_QUERY_SERVICE_NAME;

				retrieveAndQueryMetrics(jmQueryServicePath);

				/**
				 * We first request the list of all registered task managers from the job manager, and then
				 * request the respective metric dump from each task manager.
				 *
				 * <p>All stored metrics that do not belong to a registered task manager will be removed.
				 */
				CompletableFuture<Collection<Instance>> taskManagersFuture = jobManagerGateway.requestTaskManagerInstances(timeout);

				taskManagersFuture.whenCompleteAsync(
					(Collection<Instance> taskManagers, Throwable throwable) -> {
						if (throwable != null) {
							LOG.debug("Fetching list of registered TaskManagers failed.", throwable);
						} else {
							List<String> activeTaskManagers = taskManagers.stream().map(
								taskManagerInstance -> {
									final String taskManagerAddress = taskManagerInstance.getTaskManagerGateway().getAddress();
									final String tmQueryServicePath = taskManagerAddress.substring(0, taskManagerAddress.lastIndexOf('/') + 1) + MetricQueryService.METRIC_QUERY_SERVICE_NAME + "_" + taskManagerInstance.getTaskManagerID().getResourceIdString();

									retrieveAndQueryMetrics(tmQueryServicePath);

									return taskManagerInstance.getId().toString();
								}).collect(Collectors.toList());

							synchronized (metrics) {
								metrics.taskManagers.keySet().retainAll(activeTaskManagers);
							}
						}
					},
					executor);
			}
		} catch (Exception e) {
			LOG.warn("Exception while fetching metrics.", e);
		}
	}

	/**
	 * Retrieves and queries the specified QueryServiceGateway.
	 *
	 * @param queryServicePath specifying the QueryServiceGateway
	 */
	private void retrieveAndQueryMetrics(String queryServicePath) {
		final CompletableFuture<MetricQueryServiceGateway> queryServiceGatewayFuture = queryServiceRetriever.retrieveService(queryServicePath);

		queryServiceGatewayFuture.whenCompleteAsync(
			(MetricQueryServiceGateway queryServiceGateway, Throwable t) -> {
				if (t != null) {
					LOG.debug("Could not retrieve QueryServiceGateway.", t);
				} else {
					queryMetrics(queryServiceGateway);
				}
			},
			executor);
	}

	/**
	 * Query the metrics from the given QueryServiceGateway.
	 *
	 * @param queryServiceGateway to query for metrics
	 */
	private void queryMetrics(final MetricQueryServiceGateway queryServiceGateway) {
		queryServiceGateway
			.queryMetrics(timeout)
			.whenCompleteAsync(
				(MetricDumpSerialization.MetricSerializationResult result, Throwable t) -> {
					if (t != null) {
						LOG.debug("Fetching metrics failed.", t);
					} else {
						List<MetricDump> dumpedMetrics = deserializer.deserialize(result);
						synchronized (metrics) {
							for (MetricDump metric : dumpedMetrics) {
								metrics.add(metric);
							}
						}
					}
				},
				executor);
	}
}
