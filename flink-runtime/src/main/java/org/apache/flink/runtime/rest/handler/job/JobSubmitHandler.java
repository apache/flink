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
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This handler can be used to submit jobs to a Flink cluster.
 */
public final class JobSubmitHandler extends AbstractRestHandler<DispatcherGateway, JobSubmitRequestBody, JobSubmitResponseBody, EmptyMessageParameters> {

	public JobSubmitHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
			Time timeout,
			Map<String, String> headers) {
		super(localRestAddress, leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
	}

	@Override
	protected CompletableFuture<JobSubmitResponseBody> handleRequest(@Nonnull HandlerRequest<JobSubmitRequestBody, EmptyMessageParameters> request, @Nonnull DispatcherGateway gateway) throws RestHandlerException {
		JobGraph jobGraph;
		try {
			ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(request.getRequestBody().serializedJobGraph));
			jobGraph = (JobGraph) objectIn.readObject();
		} catch (Exception e) {
			throw new RestHandlerException(
				"Failed to deserialize JobGraph.",
				HttpResponseStatus.BAD_REQUEST,
				e);
		}

		return gateway.submitJob(jobGraph, timeout)
			.thenApply(ack -> new JobSubmitResponseBody("/jobs/" + jobGraph.getJobID()));
	}
}
