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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedJobMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedJobMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobsFilterQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler that returns, aggregated across jobs, a list of all available metrics or the values
 * for a set of metrics.
 *
 * <p>Specific jobs can be selected for aggregation by specifying a comma-separated list of job IDs.
 * {@code /metrics?get=X,Y&jobs=A,B}
 */
public class AggregatingJobsMetricsHandler extends AbstractAggregatingMetricsHandler<AggregatedJobMetricsParameters> {

	public AggregatingJobsMetricsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout, Map<String, String> responseHeaders,
			Executor executor,
			MetricFetcher<?> fetcher) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, AggregatedJobMetricsHeaders.getInstance(), executor, fetcher);
	}

	@Nonnull
	@Override
	Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, HandlerRequest<EmptyRequestBody, AggregatedJobMetricsParameters> request) {
		List<JobID> jobs = request.getQueryParameter(JobsFilterQueryParameter.class);
		if (jobs.isEmpty()) {
			return store.getJobs().values();
		} else {
			Collection<MetricStore.ComponentMetricStore> jobStores = new ArrayList<>(jobs.size());
			for (JobID job : jobs) {
				MetricStore.ComponentMetricStore jobMetricStore = store.getJobMetricStore(job.toString());
				if (jobMetricStore != null) {
					jobStores.add(jobMetricStore);
				}
			}
			return jobStores;
		}
	}
}
