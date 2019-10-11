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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.ApplicationStatusParameter;
import org.apache.flink.runtime.rest.messages.DiagnosticsParameter;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ShutdownMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST handler which allows to shut down the cluster.
 */
public class ShutdownHandler extends
		AbstractRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, ShutdownMessageParameters> {

	public ShutdownHandler(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<EmptyRequestBody, EmptyResponseBody, ShutdownMessageParameters> messageHeaders) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
	}

	@Override
	protected CompletableFuture<EmptyResponseBody> handleRequest(
			@Nonnull final HandlerRequest<EmptyRequestBody, ShutdownMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {
		ApplicationStatus status = null;
		String diagnosticMessage = null;
		List<ApplicationStatus> statuses = request.getQueryParameter(ApplicationStatusParameter.class);
		List<String> diagnosticMessages = request.getQueryParameter(DiagnosticsParameter.class);

		if (!statuses.isEmpty()) {
			status = statuses.get(0);
		} else {
			throw new IllegalArgumentException(ApplicationStatusParameter.APP_STATUS + " is required.");
		}
		if (!diagnosticMessages.isEmpty()) {
			diagnosticMessage = diagnosticMessages.get(0);
		}
		return gateway.shutDownCluster(status, diagnosticMessage).thenApply(ignored -> EmptyResponseBody.getInstance());
	}
}
