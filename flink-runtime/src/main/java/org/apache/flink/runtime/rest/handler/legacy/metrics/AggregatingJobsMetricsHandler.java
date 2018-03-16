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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler that returns, aggregated across all jobs, a list of all available metrics or the values
 * for a set of metrics.
 *
 * <p>Specific jobs can be selected for aggregation by specifying a comma-separated list of job IDs.
 * {@code /metrics?get=X,Y&jobs=A,B}
 */
public class AggregatingJobsMetricsHandler extends AbstractAggregatingMetricsHandler {
	public AggregatingJobsMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	protected Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, Map<String, String> pathParameters, Map<String, String> queryParameters) {
		String jobsList = queryParameters.get("jobs");
		if (jobsList == null || jobsList.isEmpty()) {
			return store.getJobs().values();
		} else {
			String[] jobs = jobsList.split(",");
			Collection<MetricStore.ComponentMetricStore> jobStores = new ArrayList<>();
			for (String job : jobs) {
				jobStores.add(store.getJobMetricStore(job));
			}
			return jobStores;
		}
	}

	@Override
	public String[] getPaths() {
		return new String[]{"/jobs/metrics"};
	}
}
