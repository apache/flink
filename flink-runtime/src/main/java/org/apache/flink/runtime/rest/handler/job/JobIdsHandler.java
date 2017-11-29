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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusesOverview;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Handler for job IDs.
 *
 * @param <T> type of the leader gateway
 */
public class JobIdsHandler<T extends RestfulGateway>
		extends AbstractRestHandler<T, EmptyRequestBody, JobIdsWithStatusesOverview, EmptyMessageParameters> {

	public JobIdsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends T> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobIdsWithStatusesOverview, EmptyMessageParameters> messageHeaders) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders);
	}

	@Override
	protected CompletableFuture<JobIdsWithStatusesOverview> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
			@Nonnull T gateway) throws RestHandlerException {

		return gateway.requestJobDetails(timeout).thenApply(
			multipleJobDetails -> new JobIdsWithStatusesOverview(
				multipleJobDetails.getJobs().stream().map(jobDetails -> Tuple2.of(jobDetails.getJobId(), jobDetails.getStatus())).collect(Collectors.toList())
			)
		);
	}
}
