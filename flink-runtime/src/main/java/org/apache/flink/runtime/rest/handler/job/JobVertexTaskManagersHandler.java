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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.JobVertexTaskManagersInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler for the job vertex task managers.
 */
public class JobVertexTaskManagersHandler extends AbstractExecutionGraphHandler<JobVertexTaskManagersInfo, JobVertexMessageParameters> {
	private MetricFetcher<?> metricFetcher;

	public JobVertexTaskManagersHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexTaskManagersInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher<?> metricFetcher) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
		this.metricFetcher = metricFetcher;
	}

	@Override
	protected JobVertexTaskManagersInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		// Build a map that groups tasks by TaskManager
		Map<String, List<AccessExecutionVertex>> taskManagerVertices = new HashMap<>();
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String taskManager = location == null ? "(unassigned)" : location.getHostname() + ":" + location.dataPort();
			List<AccessExecutionVertex> vertices = taskManagerVertices.get(taskManager);
			if (vertices == null) {
				vertices = new ArrayList<>();
				taskManagerVertices.put(taskManager, vertices);
			}

			vertices.add(vertex);
		}

		final long now = System.currentTimeMillis();

		List<JobVertexTaskManagersInfo.TaskManagersInfo> taskManagersInfoList = new ArrayList<>();
		for (Map.Entry<String, List<AccessExecutionVertex>> entry : taskManagerVertices.entrySet()) {
			String host = entry.getKey();
			List<AccessExecutionVertex> taskVertices = entry.getValue();

			int[] tasksPerState = new int[ExecutionState.values().length];

			long startTime = Long.MAX_VALUE;
			long endTime = 0;
			boolean allFinished = true;

			MutableIOMetrics counts = new MutableIOMetrics();

			for (AccessExecutionVertex vertex : taskVertices) {
				final ExecutionState state = vertex.getExecutionState();
				tasksPerState[state.ordinal()]++;

				// take the earliest start time
				long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
				if (started > 0) {
					startTime = Math.min(startTime, started);
				}

				allFinished &= state.isTerminal();
				endTime = Math.max(endTime, vertex.getStateTimestamp(state));

				counts.addIOMetrics(
					vertex.getCurrentExecutionAttempt(),
					metricFetcher,
					jobID.toString(),
					jobVertex.getJobVertexId().toString());
			}

			long duration;
			if (startTime < Long.MAX_VALUE) {
				if (allFinished) {
					duration = endTime - startTime;
				}
				else {
					endTime = -1L;
					duration = now - startTime;
				}
			}
			else {
				startTime = -1L;
				endTime = -1L;
				duration = -1L;
			}

			ExecutionState jobVertexState =
				ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, taskVertices.size());
			final JobDetailsInfo.JobVertexMetrics jobVertexMetrics = new JobDetailsInfo.JobVertexMetrics(
				counts.getNumBytesInLocal() + counts.getNumBytesInRemote(),
				counts.isNumBytesInLocalComplete() && counts.isNumBytesInRemoteComplete(),
				counts.getNumBytesOut(),
				counts.isNumBytesOutComplete(),
				counts.getNumRecordsIn(),
				counts.isNumRecordsInComplete(),
				counts.getNumRecordsOut(),
				counts.isNumRecordsOutComplete());

			Map<ExecutionState, Integer> statusCounts = new HashMap<>();
			for (ExecutionState state : ExecutionState.values()) {
				statusCounts.put(state, tasksPerState[state.ordinal()]);
			}
			taskManagersInfoList.add(new JobVertexTaskManagersInfo.TaskManagersInfo(host, jobVertexState, startTime, endTime, duration, jobVertexMetrics, statusCounts));
		}

		return new JobVertexTaskManagersInfo(jobVertexID, jobVertex.getName(), now, taskManagersInfoList);
	}
}
