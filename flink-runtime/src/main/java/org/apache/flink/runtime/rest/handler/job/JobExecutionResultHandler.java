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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExecutionResultResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Returns the {@link org.apache.flink.api.common.JobExecutionResult} for a given {@link JobID}.
 */
public class JobExecutionResultHandler
	extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobExecutionResultResponseBody, JobMessageParameters> {

	public JobExecutionResultHandler(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			JobExecutionResultHeaders.getInstance());
	}

	@Override
	protected CompletableFuture<JobExecutionResultResponseBody> handleRequest(
			@Nonnull final HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {

		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);

		final CompletableFuture<JobStatus> jobStatusFuture = gateway.requestJobStatus(jobId, timeout);

		return jobStatusFuture.thenCompose(
			jobStatus -> {
				if (jobStatus.isGloballyTerminalState()) {
					return gateway
						.requestJobResult(jobId, timeout)
						.thenApply(JobExecutionResultResponseBody::created);
				} else {
					return CompletableFuture.completedFuture(
						JobExecutionResultResponseBody.inProgress());
				}
			}).exceptionally(throwable -> {
				throw propagateException(throwable);
			});
	}

	private static CompletionException propagateException(final Throwable throwable) {
		final Throwable cause = ExceptionUtils.stripCompletionException(throwable);

		if (cause instanceof FlinkJobNotFoundException) {
			throw new CompletionException(new RestHandlerException(
				throwable.getMessage(),
				HttpResponseStatus.NOT_FOUND,
				throwable));
		} else {
			throw new CompletionException(throwable);
		}
	}
}
