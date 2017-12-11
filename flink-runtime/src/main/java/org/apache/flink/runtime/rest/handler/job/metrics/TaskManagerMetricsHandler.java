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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagerMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.TaskManagerMetricsMessageParameters;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler that returns TaskManager metrics.
 *
 * @see MetricStore#getTaskManagerMetricStore(String)
 */
public class TaskManagerMetricsHandler extends AbstractMetricsHandler<TaskManagerMetricsMessageParameters> {

	public TaskManagerMetricsHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> headers,
			final MetricFetcher metricFetcher) {
		super(localRestAddress, leaderRetriever, timeout, headers, TaskManagerMetricsHeaders.getInstance(),
			metricFetcher);
	}

	@Nullable
	@Override
	protected MetricStore.ComponentMetricStore getComponentMetricStore(
			final HandlerRequest<EmptyRequestBody, TaskManagerMetricsMessageParameters> request,
			final MetricStore metricStore) {
		final ResourceID taskManagerId = request.getPathParameter(TaskManagerIdPathParameter.class);
		return metricStore.getTaskManagerMetricStore(taskManagerId.toString());
	}

}
