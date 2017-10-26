package org.apache.flink.runtime.rest.handler.job;
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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler serving the job execution plan.
 */
public class JobPlanHandler extends AbstractExecutionGraphHandler<JobPlanInfo, JobMessageParameters> {

	public JobPlanHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		MessageHeaders<EmptyRequestBody, JobPlanInfo, JobMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor) {

		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobPlanInfo handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request, AccessExecutionGraph executionGraph) throws RestHandlerException {
		return new JobPlanInfo(executionGraph.getJsonPlan());
	}
}
