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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractJobVertexHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.JobVertexWatermarksHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricCollectionResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;


/**
 * Handler that returns the watermarks given a {@link JobID} and {@link JobVertexID}.
 */
public class JobVertexWatermarksHandler extends AbstractJobVertexHandler<MetricCollectionResponseBody, JobVertexMessageParameters> {

	private final MetricFetcher metricFetcher;

	public JobVertexWatermarksHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MetricFetcher metricFetcher,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(leaderRetriever,
			timeout,
			responseHeaders,
			JobVertexWatermarksHeaders.INSTANCE,
			executionGraphCache,
			executor);
		this.metricFetcher = metricFetcher;
	}

	@Override
	protected MetricCollectionResponseBody handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionJobVertex jobVertex) throws RestHandlerException {

		String jobID = request.getPathParameter(JobIDPathParameter.class).toString();
		String taskID = jobVertex.getJobVertexId().toString();

		metricFetcher.update();
		MetricStore.TaskMetricStore taskMetricStore = metricFetcher.getMetricStore().getTaskMetricStore(jobID, taskID);
		if (taskMetricStore == null) {
			return new MetricCollectionResponseBody(Collections.emptyList());
		}

		AccessExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
		List<Metric> metrics = new ArrayList<>(taskVertices.length);

		for (AccessExecutionVertex taskVertex : taskVertices) {
			String id = taskVertex.getParallelSubtaskIndex() + "." + MetricNames.IO_CURRENT_INPUT_WATERMARK;
			String watermarkValue = taskMetricStore.getMetric(id);
			if (watermarkValue != null) {
				metrics.add(new Metric(id, watermarkValue));
			}
		}

		return new MetricCollectionResponseBody(metrics);
	}
}
