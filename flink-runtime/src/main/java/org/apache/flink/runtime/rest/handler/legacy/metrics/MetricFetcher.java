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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
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
public class MetricFetcher<T extends RestfulGateway> {
	private static final Logger LOG = LoggerFactory.getLogger(MetricFetcher.class);

	private final GatewayRetriever<T> retriever;
	private final MetricQueryServiceRetriever queryServiceRetriever;
	private final Executor executor;
	private final Time timeout;

	private final MetricStore metrics = new MetricStore();
	private final MetricDumpDeserializer deserializer = new MetricDumpDeserializer();

	private long lastUpdateTime;

	public MetricFetcher(
			GatewayRetriever<T> retriever,
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
		LOG.debug("Start fetching metrics.");

		try {
			Optional<T> optionalLeaderGateway = retriever.getNow();
			if (optionalLeaderGateway.isPresent()) {
				final T leaderGateway = optionalLeaderGateway.get();

				/*
				 * Remove all metrics that belong to a job that is not running and no longer archived.
				 */
				CompletableFuture<MultipleJobsDetails> jobDetailsFuture = leaderGateway.requestMultipleJobDetails(timeout);

				jobDetailsFuture.whenCompleteAsync(
					(MultipleJobsDetails jobDetails, Throwable throwable) -> {
						if (throwable != null) {
							LOG.debug("Fetching of JobDetails failed.", throwable);
						} else {
							ArrayList<String> toRetain = new ArrayList<>(jobDetails.getJobs().size());
							for (JobDetails job : jobDetails.getJobs()) {
								toRetain.add(job.getJobId().toString());
							}
							metrics.retainJobs(toRetain);
						}
					},
					executor);

				CompletableFuture<Collection<String>> queryServicePathsFuture = leaderGateway.requestMetricQueryServicePaths(timeout);

				queryServicePathsFuture.whenCompleteAsync(
					(Collection<String> queryServicePaths, Throwable throwable) -> {
						if (throwable != null) {
							LOG.warn("Requesting paths for query services failed.", throwable);
						} else {
							for (String queryServicePath : queryServicePaths) {
								retrieveAndQueryMetrics(queryServicePath);
							}
						}
					},
					executor);

				// TODO: Once the old code has been ditched, remove the explicit TaskManager query service discovery
				// TODO: and return it as part of requestQueryServicePaths. Moreover, change the MetricStore such that
				// TODO: we don't have to explicitly retain the valid TaskManagers, e.g. letting it be a cache with expiry time
				CompletableFuture<Collection<Tuple2<ResourceID, String>>> taskManagerQueryServicePathsFuture = leaderGateway
					.requestTaskManagerMetricQueryServicePaths(timeout);

				taskManagerQueryServicePathsFuture.whenCompleteAsync(
					(Collection<Tuple2<ResourceID, String>> queryServicePaths, Throwable throwable) -> {
						if (throwable != null) {
							LOG.warn("Requesting TaskManager's path for query services failed.", throwable);
						} else {
							List<String> taskManagersToRetain = queryServicePaths
								.stream()
								.map(
									(Tuple2<ResourceID, String> tuple) -> {
										retrieveAndQueryMetrics(tuple.f1);
										return tuple.f0.getResourceIdString();
									}
								).collect(Collectors.toList());

							metrics.retainTaskManagers(taskManagersToRetain);
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
		LOG.debug("Retrieve metric query service gateway for {}", queryServicePath);

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
		LOG.debug("Query metrics for {}.", queryServiceGateway.getAddress());

		queryServiceGateway
			.queryMetrics(timeout)
			.whenCompleteAsync(
				(MetricDumpSerialization.MetricSerializationResult result, Throwable t) -> {
					if (t != null) {
						LOG.debug("Fetching metrics failed.", t);
					} else {
						metrics.addAll(deserializer.deserialize(result));
					}
				},
				executor);
	}
}
