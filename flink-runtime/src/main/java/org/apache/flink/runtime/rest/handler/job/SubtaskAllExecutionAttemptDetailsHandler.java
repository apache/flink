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
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAllAttemptsInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptInfo;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler providing details about all execution attempts of a subtask.
 */
public class SubtaskAllExecutionAttemptDetailsHandler extends AbstractSubtaskHandler<SubtaskExecutionAllAttemptsInfo, SubtaskMessageParameters> {

	public SubtaskAllExecutionAttemptDetailsHandler(
		CompletableFuture<String> localRestAddress,
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, SubtaskExecutionAllAttemptsInfo, SubtaskMessageParameters> messageHeaders,
		ExecutionGraphCache executionGraphCache,
		Executor executor) {

		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected SubtaskExecutionAllAttemptsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, SubtaskMessageParameters> request,
			AccessExecutionVertex executionVertex) throws RestHandlerException {

		final AccessExecution execution = executionVertex.getCurrentExecutionAttempt();

		final int currentAttemptNum = execution.getAttemptNumber();

		final List<SubtaskExecutionAttemptInfo> allAttempts = new ArrayList<>();
		allAttempts.add(SubtaskExecutionAttemptInfo.create(execution));

		if (currentAttemptNum > 0) {
			for (int i = currentAttemptNum - 1; i >= 0; i--) {
				final AccessExecution currentExecution = executionVertex.getPriorExecutionAttempt(i);
				if (currentExecution != null) {
					allAttempts.add(SubtaskExecutionAttemptInfo.create(currentExecution));
				}
			}
		}

		return new SubtaskExecutionAllAttemptsInfo(allAttempts);
	}
}
