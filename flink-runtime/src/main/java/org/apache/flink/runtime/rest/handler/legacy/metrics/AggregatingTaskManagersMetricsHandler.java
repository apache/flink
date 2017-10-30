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
 * Request handler that returns, aggregated across all task managers, a list of all available metrics or the values for
 * a set of metrics.
 *
 * <p>Specific taskmanagers can be selected for aggregation by specifying a comma-separated list of taskmanager IDs.
 * {@code /metrics?get=X,Y&taskmanagers=A,B}
 */
public class AggregatingTaskManagersMetricsHandler extends AbstractAggregatingMetricsHandler {
	public AggregatingTaskManagersMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	protected Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, Map<String, String> pathParameters, Map<String, String> queryParameters) {
		String taskmanagersList = queryParameters.get("taskmanagers");
		if (taskmanagersList == null || taskmanagersList.isEmpty()) {
			return store.getTaskManagers().values();
		} else {
			String[] taskmanagers = taskmanagersList.split(",");
			Collection<MetricStore.TaskManagerMetricStore> taskmanagerStores = new ArrayList<>();
			for (String taskmanager : taskmanagers) {
				taskmanagerStores.add(store.getTaskManagerMetricStore(taskmanager));
			}
			return taskmanagerStores;
		}
	}

	@Override
	public String[] getPaths() {
		return new String[]{"/taskmanagers/metrics"};
	}
}
