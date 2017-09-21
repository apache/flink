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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.LegacyRestHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

/**
 * Request handler for the CANCEL request.
 */
public class JobCancellationHandler extends AbstractJsonRequestHandler implements LegacyRestHandler<DispatcherGateway, EmptyResponseBody, JobMessageParameters> {

	private static final String JOB_CONCELLATION_REST_PATH = "/jobs/:jobid/cancel";
	private static final String JOB_CONCELLATION_YARN_REST_PATH = "/jobs/:jobid/yarn-cancel";

	private final Time timeout;

	public JobCancellationHandler(Executor executor, Time timeout) {
		super(executor);
		this.timeout = Preconditions.checkNotNull(timeout);
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOB_CONCELLATION_REST_PATH, JOB_CONCELLATION_YARN_REST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					JobID jobId = new JobID(StringUtils.hexStringToByte(pathParams.get("jobid")));
					if (jobManagerGateway != null) {
						jobManagerGateway.cancelJob(jobId, timeout);
						return "{}";
					}
					else {
						throw new Exception("No connection to the leading JobManager.");
					}
				}
				catch (Exception e) {
					throw new CompletionException(new FlinkException("Failed to cancel the job with id: "  + pathParams.get("jobid"), e));
				}
			},
			executor);
	}

	@Override
	public CompletableFuture<EmptyResponseBody> handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request, DispatcherGateway gateway) {
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		CompletableFuture<Acknowledge> cancelFuture = gateway.cancelJob(jobId, timeout);

		return cancelFuture.handle(
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
