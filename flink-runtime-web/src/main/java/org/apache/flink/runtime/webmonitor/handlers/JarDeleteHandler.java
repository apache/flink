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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

/**
 * Handles requests for deletion of jars.
 */
public class JarDeleteHandler
		extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, EmptyResponseBody, JarDeleteMessageParameters> {

	private final Path jarDir;

	private final Executor executor;

	public JarDeleteHandler(
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<EmptyRequestBody, EmptyResponseBody, JarDeleteMessageParameters> messageHeaders,
			final Path jarDir,
			final Executor executor) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.jarDir = requireNonNull(jarDir);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<EmptyResponseBody> handleRequest(
			@Nonnull final HandlerRequest<EmptyRequestBody, JarDeleteMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {

		final String jarId = request.getPathParameter(JarIdPathParameter.class);
		return CompletableFuture.supplyAsync(() -> {
			final Path jarToDelete = jarDir.resolve(jarId);
			if (!Files.exists(jarToDelete)) {
				throw new CompletionException(new RestHandlerException(
					String.format("File %s does not exist in %s.", jarId, jarDir),
					HttpResponseStatus.BAD_REQUEST));
			} else {
				try {
					Files.delete(jarToDelete);
					return EmptyResponseBody.getInstance();
				} catch (final IOException e) {
					throw new CompletionException(new RestHandlerException(
						String.format("Failed to delete jar %s.", jarToDelete),
						HttpResponseStatus.INTERNAL_SERVER_ERROR,
						e));
				}
			}
		}, executor);
	}
}
