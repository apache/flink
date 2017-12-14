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
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler for the job vertex accumulators.
 */
public class JobVertexAccumulatorsHandler extends AbstractExecutionGraphHandler<JobVertexAccumulatorsInfo, JobVertexMessageParameters> {

	public JobVertexAccumulatorsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexAccumulatorsInfo, JobVertexMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobVertexAccumulatorsInfo handleRequest(HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request, AccessExecutionGraph executionGraph) throws RestHandlerException {
		JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);
		AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);

		if (null != jobVertex) {
			StringifiedAccumulatorResult[] accs = jobVertex.getAggregatedUserAccumulatorsStringified();
			ArrayList<JobVertexAccumulatorsInfo.UserAccumulator> userAccumulatorList = new ArrayList<>(accs.length);

			for (StringifiedAccumulatorResult acc : accs) {
				userAccumulatorList.add(
					new JobVertexAccumulatorsInfo.UserAccumulator(
						acc.getName(),
						acc.getType(),
						acc.getValue()));
			}

			return new JobVertexAccumulatorsInfo(jobVertex.getJobVertexId().toString(), userAccumulatorList);
		} else {
			throw new RestHandlerException("There is no accumulator for vertex " + jobVertexID + '.', HttpResponseStatus.NOT_FOUND);
		}
	}
}
