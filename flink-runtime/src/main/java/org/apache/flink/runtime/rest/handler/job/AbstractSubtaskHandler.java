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
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.Executor;


/**
 * Base class for request handlers whose response depends on a specific subtask (defined
 * via the "{@link SubtaskIndexPathParameter#KEY}" in a specific job vertex, (defined
 * via the "{@link JobVertexIdPathParameter#KEY}" parameter) in a specific job,
 * defined via (defined via the "{@link JobIDPathParameter#KEY}" parameter).
 *
 *
 * @param <R> the response type
 * @param <M> the message parameters type
 */
public abstract class AbstractSubtaskHandler<R extends ResponseBody, M extends SubtaskMessageParameters> extends AbstractJobVertexHandler<R, M> {

	/**
	 * Instantiates a new Abstract job vertex handler.
	 *
	 * @param leaderRetriever     the leader retriever
	 * @param timeout             the timeout
	 * @param responseHeaders     the response headers
	 * @param messageHeaders      the message headers
	 * @param executionGraphCache the execution graph cache
	 * @param executor            the executor
	 */
	protected AbstractSubtaskHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout, Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {

		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected R handleRequest(
			HandlerRequest<EmptyRequestBody, M> request,
			AccessExecutionJobVertex jobVertex) throws RestHandlerException {

		final Integer subtaskIndex = request.getPathParameter(SubtaskIndexPathParameter.class);
		final AccessExecutionVertex[] executionVertices = jobVertex.getTaskVertices();

		if (subtaskIndex >= executionVertices.length || subtaskIndex < 0) {
			throw new RestHandlerException("Invalid subtask index for vertex " + jobVertex.getJobVertexId(), HttpResponseStatus.NOT_FOUND);
		}

		return handleRequest(request, executionVertices[subtaskIndex]);
	}

	/**
	 * Called for each request after the corresponding {@link AccessExecutionVertex} has been retrieved from the
	 * {@link AccessExecutionJobVertex}.
	 *
	 * @param request         the request
	 * @param executionVertex the execution vertex
	 * @return the response
	 * @throws RestHandlerException the rest handler exception
	 */
	protected abstract R handleRequest(HandlerRequest<EmptyRequestBody, M> request, AccessExecutionVertex executionVertex) throws RestHandlerException;
}
