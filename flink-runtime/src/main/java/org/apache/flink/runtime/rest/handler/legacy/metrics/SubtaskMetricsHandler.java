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

import org.apache.flink.runtime.rest.handler.legacy.SubtaskExecutionAttemptDetailsHandler;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler that returns, aggregated across all subtasks of a single tasks, a list of all available metrics or the
 * values for a set of metrics.
 *
 * <p>If the query parameters do not contain a "get" parameter the list of all metrics is returned.
 * {@code {"available": [ { "name" : "X", "id" : "X" } ] } }
 *
 * <p>If the query parameters do contain a "get" parameter, a comma-separated list of metric names is expected as a value.
 * {@code /metrics?get=X,Y}
 * The handler will then return a list containing the values of the requested metrics.
 * {@code [ { "id" : "X", "value" : "S" }, { "id" : "Y", "value" : "T" } ] }
 *
 */
public class SubtaskMetricsHandler extends AbstractMetricsHandler {
	private static final String SUBTASK_METRICS_REST_PATH = "/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/metrics";

	public SubtaskMetricsHandler(Executor executor, MetricFetcher fetcher) {
		super(executor, fetcher);
	}

	@Override
	public String[] getPaths() {
		return new String[]{SUBTASK_METRICS_REST_PATH};
	}

	@Override
	protected Map<String, String> getMapFor(Map<String, String> pathParams, MetricStore metrics) {
		String subtaskNumString = pathParams.get(SubtaskExecutionAttemptDetailsHandler.PARAMETER_SUBTASK_INDEX);
		int subtaskNum;
		try {
			subtaskNum = Integer.valueOf(subtaskNumString);
		} catch (NumberFormatException nfe) {
			return null;
		}
		MetricStore.ComponentMetricStore subtask = metrics.getSubtaskMetricStore(
			pathParams.get(JobMetricsHandler.PARAMETER_JOB_ID),
			pathParams.get(JobVertexMetricsHandler.PARAMETER_VERTEX_ID),
			subtaskNum);
		return subtask != null
			? subtask.metrics
			: null;
	}
}
