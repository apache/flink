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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.OperatorEdgeDescriptor;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobOperators;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler returning the details of all operators for the specified job.
 */
public class JobOperatorsHandler extends AbstractExecutionGraphHandler<JobOperators, JobMessageParameters>{
	public JobOperatorsHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> headers,
		MessageHeaders<EmptyRequestBody, JobOperators, JobMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor) {

		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			headers,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobOperators handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request, AccessExecutionGraph executionGraph) throws RestHandlerException {
		Collection<JobOperators.JobOperator> jobOperatorList = new ArrayList<>();
		for (AccessExecutionJobVertex accessExecutionJobVertex : executionGraph.getVerticesTopologically()) {
			for (OperatorDescriptor od: accessExecutionJobVertex.getOperatorDescriptors()) {
				JobOperators.JobOperator jobOperator = createJobOperator(accessExecutionJobVertex.getJobVertexId(), od);
				jobOperatorList.add(jobOperator);
			}
		}
		return new JobOperators(jobOperatorList);
	}

	private static JobOperators.JobOperator createJobOperator(JobVertexID jobVertexID, OperatorDescriptor operatorDescriptor){
		List<OperatorEdgeDescriptor> inputs = operatorDescriptor.getInputs();
		Collection<JobOperators.OperatorEdgeInfo> operatorEdfInfos = new ArrayList<>(inputs.size());
		for (OperatorEdgeDescriptor oed: inputs) {
			operatorEdfInfos.add(new JobOperators.OperatorEdgeInfo(oed.getSourceOperator(),
				oed.getPartitionerDescriptor(), oed.getTypeNumber()));
		}
		return new JobOperators.JobOperator(
			jobVertexID,
			operatorDescriptor.getOperatorID(),
			operatorDescriptor.getOperatorName(),
			operatorEdfInfos
		);
	}
}
