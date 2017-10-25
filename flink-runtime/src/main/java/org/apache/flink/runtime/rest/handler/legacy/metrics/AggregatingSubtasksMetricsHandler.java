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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobMetricsHandler.PARAMETER_JOB_ID;
import static org.apache.flink.runtime.rest.handler.legacy.metrics.JobVertexMetricsHandler.PARAMETER_VERTEX_ID;

/**
 * Request handler that returns, aggregated across all subtasks, a list of all available metrics or the values
 * for a set of metrics.
 */
public class AggregatingSubtasksMetricsHandler extends AbstractAggregatingMetricsHandler {
	public AggregatingSubtasksMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	protected Collection<? extends MetricStore.ComponentMetricStore> getStores(MetricStore store, Map<String, String> pathParameters, Map<String, String> queryParameters) {
		String jobID = pathParameters.get(PARAMETER_JOB_ID);
		String taskID = pathParameters.get(PARAMETER_VERTEX_ID);
		if (taskID == null) {
			return Collections.emptyList();
		}
		String subtasksList = queryParameters.get("subtasks");
		if (subtasksList == null || subtasksList.isEmpty()) {
			return store.getTaskMetricStore(jobID, taskID).getAllSubtaskMetricStores();
		} else {
			String[] subtasks = subtasksList.split(",");
			Collection<MetricStore.ComponentMetricStore> subtaskStores = new ArrayList<>();
			for (String subtask : subtasks) {
				int subtaskNum;
				try {
					subtaskNum = Integer.valueOf(subtask);
				} catch (NumberFormatException nfe) {
					log.warn("Invalid subtask index specified {}. Not a number.", subtask, nfe);
					continue;
				}
				subtaskStores.add(store.getSubtaskMetricStore(jobID, taskID, subtaskNum));
			}
			return subtaskStores;
		}
	}

	@Override
	public String[] getPaths() {
		return new String[]{"/jobs/:jobid/vertices/:vertexid/subtasks/metrics"};
	}
}
