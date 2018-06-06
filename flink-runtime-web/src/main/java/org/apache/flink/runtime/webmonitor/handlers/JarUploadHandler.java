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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.FileUpload;
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
 * Handles .jar file uploads.
 */
public class JarUploadHandler extends
		AbstractRestHandler<RestfulGateway, FileUpload, JarUploadResponseBody, EmptyMessageParameters> {

	private final Path jarDir;

	private final Executor executor;

	public JarUploadHandler(
			final CompletableFuture<String> localRestAddress,
			final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			final Time timeout,
			final Map<String, String> responseHeaders,
			final MessageHeaders<FileUpload, JarUploadResponseBody, EmptyMessageParameters> messageHeaders,
			final Path jarDir,
			final Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.jarDir = requireNonNull(jarDir);
		this.executor = requireNonNull(executor);
	}

	@Override
	protected CompletableFuture<JarUploadResponseBody> handleRequest(
			@Nonnull final HandlerRequest<FileUpload, EmptyMessageParameters> request,
			@Nonnull final RestfulGateway gateway) throws RestHandlerException {

		final FileUpload fileUpload = request.getRequestBody();
		return CompletableFuture.supplyAsync(() -> {
			if (!fileUpload.getPath().getFileName().toString().endsWith(".jar")) {
				deleteUploadedFile(fileUpload);
				throw new CompletionException(new RestHandlerException(
					"Only Jar files are allowed.",
					HttpResponseStatus.BAD_REQUEST));
			} else {
				final Path destination = jarDir.resolve(fileUpload.getPath().getFileName());
				try {
					Files.move(fileUpload.getPath(), destination);
				} catch (IOException e) {
					deleteUploadedFile(fileUpload);
					throw new CompletionException(new RestHandlerException(
						String.format("Could not move uploaded jar file [%s] to [%s].",
							fileUpload.getPath(),
							destination),
						HttpResponseStatus.INTERNAL_SERVER_ERROR,
						e));
				}
				return new JarUploadResponseBody(fileUpload.getPath()
					.normalize()
					.toString());
			}
		}, executor);
	}

	private void deleteUploadedFile(final FileUpload fileUpload) {
		try {
			Files.delete(fileUpload.getPath());
		} catch (IOException e) {
			log.error("Failed to delete file {}.", fileUpload.getPath(), e);
		}
	}
}
