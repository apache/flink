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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler for the job vertex details.
 */
public class JobVertexDetailsHandler extends AbstractExecutionGraphHandler<JobVertexDetailsInfo, JobVertexMessageParameters> {
	private final MetricFetcher<? extends RestfulGateway> metricFetcher;

	public JobVertexDetailsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexDetailsInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher<? extends RestfulGateway> metricFetcher) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
		this.metricFetcher = metricFetcher;
	}

	@Override
	protected JobVertexDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionGraph executionGraph) throws NotFoundException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		if (jobVertex == null) {
			throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
		}

		List<JobVertexDetailsInfo.VertexTaskDetail> subtasks = new ArrayList<>();
		final long now = System.currentTimeMillis();
		int num = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			final ExecutionState status = vertex.getExecutionState();

			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();

			long startTime = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
			if (startTime == 0) {
				startTime = -1;
			}
			long endTime = status.isTerminal() ? vertex.getStateTimestamp(status) : -1;
			long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

			MutableIOMetrics counts = new MutableIOMetrics();
			counts.addIOMetrics(
				vertex.getCurrentExecutionAttempt(),
				metricFetcher,
				jobID.toString(),
				jobVertex.getJobVertexId().toString());
			subtasks.add(new JobVertexDetailsInfo.VertexTaskDetail(
				num,
				status,
				vertex.getCurrentExecutionAttempt().getAttemptNumber(),
				locationString,
				startTime,
				endTime,
				duration,
				new IOMetricsInfo(
					counts.getNumBytesInLocal() + counts.getNumBytesInRemote(),
					counts.isNumBytesInLocalComplete() && counts.isNumBytesInRemoteComplete(),
					counts.getNumBytesOut(),
					counts.isNumBytesOutComplete(),
					counts.getNumRecordsIn(),
					counts.isNumRecordsInComplete(),
					counts.getNumRecordsOut(),
					counts.isNumRecordsOutComplete())));

			num++;
		}

		return new JobVertexDetailsInfo(
			jobVertex.getJobVertexId(),
			jobVertex.getName(),
			jobVertex.getParallelism(),
			now,
			subtasks);
	}
}
