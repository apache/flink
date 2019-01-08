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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Adapter for Flink's legacy REST handlers.
 *
 * @param <T> type of the gateway
 * @param <R> type of the REST response
 * @param <M> type of the MessageParameters
 */
public class LegacyRestHandlerAdapter<T extends RestfulGateway, R extends ResponseBody, M extends MessageParameters> extends AbstractRestHandler<T, EmptyRequestBody, R, M> {

	private final LegacyRestHandler<T, R, M> legacyRestHandler;

	public LegacyRestHandlerAdapter(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<T> leaderRetriever,
			Time timeout,
			Map<String, String> headers,
			MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
			LegacyRestHandler<T, R, M> legacyRestHandler) {
		super(localRestAddress, leaderRetriever, timeout, headers, messageHeaders);

		this.legacyRestHandler = Preconditions.checkNotNull(legacyRestHandler);
	}

	@Override
	protected CompletableFuture<R> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, M> request, @Nonnull T gateway) throws RestHandlerException {
		return legacyRestHandler.handleRequest(request, gateway);
	}
}
