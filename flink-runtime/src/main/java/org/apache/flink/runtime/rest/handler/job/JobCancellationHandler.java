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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.TerminationModeQueryParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/**
 * Request handler for the cancel and stop request.
 */
public class JobCancellationHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, JobCancellationMessageParameters> {

	private final TerminationModeQueryParameter.TerminationMode defaultTerminationMode;

	public JobCancellationHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody, EmptyResponseBody, JobCancellationMessageParameters> messageHeaders,
			TerminationModeQueryParameter.TerminationMode defaultTerminationMode) {
		super(leaderRetriever, timeout, headers, messageHeaders);

		this.defaultTerminationMode = Preconditions.checkNotNull(defaultTerminationMode);
	}

	@Override
	public CompletableFuture<EmptyResponseBody> handleRequest(HandlerRequest<EmptyRequestBody, JobCancellationMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		final List<TerminationModeQueryParameter.TerminationMode> terminationModes = request.getQueryParameter(TerminationModeQueryParameter.class);
		final TerminationModeQueryParameter.TerminationMode terminationMode;

		if (terminationModes.isEmpty()) {
			terminationMode = defaultTerminationMode;
		} else {
			// picking the first termination mode value
			terminationMode = terminationModes.get(0);
		}

		final CompletableFuture<Acknowledge> terminationFuture;

		switch (terminationMode) {
			case CANCEL:
				terminationFuture = gateway.cancelJob(jobId, timeout);
				break;
			case STOP:
				throw new RestHandlerException("The termination mode \"stop\" has been removed. For " +
				"an ungraceful shutdown, please use \"cancel\" instead. For a graceful shutdown, " +
				"please use \"jobs/:jobId/stop\" instead." , HttpResponseStatus.PERMANENT_REDIRECT);
			default:
				terminationFuture = FutureUtils.completedExceptionally(new RestHandlerException("Unknown termination mode " + terminationMode + '.', HttpResponseStatus.BAD_REQUEST));
		}

		return terminationFuture.handle(
			(Acknowledge ack, Throwable throwable) -> {
				if (throwable != null) {
					Throwable error = ExceptionUtils.stripCompletionException(throwable);

					if (error instanceof TimeoutException) {
						throw new CompletionException(
							new RestHandlerException(
								"Job cancellation timed out.",
								HttpResponseStatus.REQUEST_TIMEOUT, error));
					} else if (error instanceof FlinkJobNotFoundException) {
						throw new CompletionException(
							new RestHandlerException(
								"Job could not be found.",
								HttpResponseStatus.NOT_FOUND, error));
					} else {
						throw new CompletionException(
							new RestHandlerException(
								"Job cancellation failed: " + error.getMessage(),
								HttpResponseStatus.INTERNAL_SERVER_ERROR, error));
					}
				} else {
					return EmptyResponseBody.getInstance();
				}
			});
	}
}
