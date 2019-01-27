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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorEdgeDescriptor;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobVerticesInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


/**
 * Handler returning the details of all vertices for the specified job.
 * Warning: if job is complex, metrics so many, jobManager will oom.
 */
public class JobVerticesInfoHandler extends AbstractExecutionGraphHandler<JobVerticesInfo, JobMessageParameters> implements JsonArchivist {

	private final MetricFetcher<?> metricFetcher;

	public JobVerticesInfoHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, JobVerticesInfo, JobMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor,
		MetricFetcher<?> metricFetcher) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);

		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
	}

	@Override
	protected JobVerticesInfo handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
			AccessExecutionGraph executionGraph) throws RestHandlerException {
		return createJobVerticesInfo(executionGraph, metricFetcher);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody json = createJobVerticesInfo(graph, metricFetcher);
		String path = getMessageHeaders().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singleton(new ArchivedJson(path, json));
	}

	private static JobVerticesInfo createJobVerticesInfo(AccessExecutionGraph executionGraph,
			@Nullable MetricFetcher<?> metricFetcher) {
		Collection<JobVerticesInfo.JobVertex> jobVertices = new ArrayList<>(executionGraph.getAllVertices().size());
		Collection<JobVerticesInfo.JobOperator> jobOperators = new ArrayList<>();
		metricFetcher.update();
		for (AccessExecutionJobVertex accessExecutionJobVertex : executionGraph.getVerticesTopologically()) {
			final JobVerticesInfo.JobVertex jobVertex = createJobVertex(
				accessExecutionJobVertex,
				executionGraph.getJobID(),
				metricFetcher,
				jobOperators);
			jobVertices.add(jobVertex);
		}
		return new JobVerticesInfo(jobVertices, jobOperators);
	}

	private static JobVerticesInfo.JobVertex createJobVertex(AccessExecutionJobVertex ejv,
			JobID jobId, MetricFetcher<?> metricFetcher, Collection<JobVerticesInfo.JobOperator> jobOperators) {
		Collection<Map<String, String>> subTaskMetrics = new ArrayList<>();
		MutableIOMetrics counts = new MutableIOMetrics();
		for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
			MetricStore.ComponentMetricStore subTaskMetric = metricFetcher.getMetricStore()
					.getSubtaskMetricStore(jobId.toString(),
						ejv.getJobVertexId().toString(), vertex.getCurrentExecutionAttempt().getParallelSubtaskIndex());
			counts.addIOMetrics(
				vertex.getCurrentExecutionAttempt(),
				metricFetcher,
				jobId.toString(),
				ejv.getJobVertexId().toString());
			if (subTaskMetric != null) {
				subTaskMetrics.add(subTaskMetric.getMetrics());
			} else {
				break;
			}
		}
		for (OperatorDescriptor od: ejv.getOperatorDescriptors()) {
			JobVerticesInfo.JobOperator jobOperator = createJobOperator(ejv.getJobVertexId(), od);
			jobOperators.add(jobOperator);
		}
		final IOMetricsInfo jobVertexMetrics = new IOMetricsInfo(counts);
		return new JobVerticesInfo.JobVertex(
			ejv.getJobVertexId(),
			ejv.getName(),
			ejv.getParallelism(),
			subTaskMetrics,
			jobVertexMetrics);
	}

	private static JobVerticesInfo.JobOperator createJobOperator(JobVertexID jobVertexID, OperatorDescriptor operatorDescriptor){
		List<OperatorEdgeDescriptor> inputs = operatorDescriptor.getInputs();
		Collection<JobVerticesInfo.OperatorEdgeInfo> operatorEdfInfos = new ArrayList<>(inputs.size());
		for (OperatorEdgeDescriptor oed: inputs) {
			operatorEdfInfos.add(new JobVerticesInfo.OperatorEdgeInfo(oed.getSourceOperator(),
				oed.getPartitionerDescriptor(), oed.getTypeNumber()));
		}
		return new JobVerticesInfo.JobOperator(
			jobVertexID,
			operatorDescriptor.getOperatorID(),
			operatorDescriptor.getOperatorName(),
			operatorEdfInfos,
			operatorDescriptor.getOperatorMetricsName()
			);
	}
}
