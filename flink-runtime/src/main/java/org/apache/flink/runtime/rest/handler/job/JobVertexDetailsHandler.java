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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler for the job vertex details.
 */
public class JobVertexDetailsHandler extends AbstractExecutionGraphHandler<JobVertexDetailsInfo, JobVertexMessageParameters> implements JsonArchivist {
	private final MetricFetcher<? extends RestfulGateway> metricFetcher;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final Time timeout;

	public JobVertexDetailsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexDetailsInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher<? extends RestfulGateway> metricFetcher,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
		this.metricFetcher = metricFetcher;
		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
		this.timeout = timeout;

	}

	@Override
	protected JobVertexDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		if (jobVertex == null) {
			throw new NotFoundException(String.format("JobVertex %s not found", jobVertexID));
		}
		return createJobVertexDetailsInfo(jobVertex, jobID, metricFetcher, resourceManagerGatewayRetriever, timeout);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		Collection<? extends AccessExecutionJobVertex> vertices = graph.getAllVertices().values();
		List<ArchivedJson> archive = new ArrayList<>(vertices.size());
		for (AccessExecutionJobVertex task : vertices) {
			ResponseBody json = createJobVertexDetailsInfo(task, graph.getJobID(), metricFetcher, resourceManagerGatewayRetriever, timeout);
			String path = getMessageHeaders().getTargetRestEndpointURL()
				.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
				.replace(':' + JobVertexIdPathParameter.KEY, task.getJobVertexId().toString());
			archive.add(new ArchivedJson(path, json));
		}
		return archive;
	}

	private JobVertexDetailsInfo createJobVertexDetailsInfo(AccessExecutionJobVertex jobVertex, JobID jobID,
																@Nullable MetricFetcher<?> metricFetcher,
																GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
																Time timeout) {
		ResourceManagerGateway resourceManagerGateway = null;
		try {
			resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);
		} catch (RestHandlerException ignore) {
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
			String resourceIdStr = "";
			String logfileName = "";
			String stdoutFileName = "";
			TaskManagerLocation taskManagerLocation = vertex.getCurrentAssignedResourceLocation();
			if (taskManagerLocation != null) {
				ResourceID resourceId = taskManagerLocation.getResourceID();
				resourceIdStr = resourceId.toString();
				if (resourceManagerGateway != null) {
					try {
						Tuple2<String, String> logAndStoutFileName = resourceManagerGateway.requestTmLogAndStdoutFileName(resourceId, timeout)
							.get(timeout.toMilliseconds(), timeout.getUnit());
						logfileName = logAndStoutFileName.f0;
						stdoutFileName = logAndStoutFileName.f1;
					} catch (Exception ignore) {
					}
				}
			}
			subtasks.add(new JobVertexDetailsInfo.VertexTaskDetail(
				num,
				status,
				vertex.getCurrentExecutionAttempt().getAttemptNumber(),
				locationString,
				startTime,
				endTime,
				duration,
				new IOMetricsInfo(counts),
				resourceIdStr,
				logfileName,
				stdoutFileName));

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
