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
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Base class for all {@link AccessExecutionGraph} based REST handlers.
 *
 * @param <R> response type
 */
public abstract class AbstractExecutionGraphHandler<R extends ResponseBody> extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, R, JobMessageParameters> {

	private final ExecutionGraphCache executionGraphCache;

	private final Executor executor;

	protected AbstractExecutionGraphHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			MessageHeaders<EmptyRequestBody, R, JobMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, messageHeaders);

		this.executionGraphCache = Preconditions.checkNotNull(executionGraphCache);
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Override
	protected CompletableFuture<R> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		CompletableFuture<AccessExecutionGraph> executionGraphFuture = executionGraphCache.getExecutionGraph(jobId, gateway);

		return executionGraphFuture.thenApplyAsync(
			this::handleRequest,
			executor);
	}

	protected abstract R handleRequest(AccessExecutionGraph executionGraph);
}
