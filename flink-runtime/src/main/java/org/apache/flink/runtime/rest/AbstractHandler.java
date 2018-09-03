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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.FileUploads;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}.
 *
 * <p>Subclasses must be thread-safe
 *
 * @param <T> type of the leader gateway
 * @param <R> type of the incoming request
 * @param <M> type of the message parameters
 */
public abstract class AbstractHandler<T extends RestfulGateway, R extends RequestBody, M extends MessageParameters> extends RedirectHandler<T> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	protected static final ObjectMapper MAPPER = RestMapperUtils.getStrictObjectMapper();

	private final UntypedResponseMessageHeaders<R, M> untypedResponseMessageHeaders;

	protected AbstractHandler(
			@Nonnull CompletableFuture<String> localAddressFuture,
			@Nonnull GatewayRetriever<? extends T> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders,
			@Nonnull UntypedResponseMessageHeaders<R, M> untypedResponseMessageHeaders) {
		super(localAddressFuture, leaderRetriever, timeout, responseHeaders);

		this.untypedResponseMessageHeaders = Preconditions.checkNotNull(untypedResponseMessageHeaders);
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext ctx, Routed routed, T gateway) throws Exception {
		if (log.isTraceEnabled()) {
			log.trace("Received request " + routed.request().getUri() + '.');
		}

		final HttpRequest httpRequest = routed.request();

		FileUploads uploadedFiles = null;
		try {
			if (!(httpRequest instanceof FullHttpRequest)) {
				// The RestServerEndpoint defines a HttpObjectAggregator in the pipeline that always returns
				// FullHttpRequests.
				log.error("Implementation error: Received a request that wasn't a FullHttpRequest.");
				throw new RestHandlerException("Bad request received.", HttpResponseStatus.BAD_REQUEST);
			}

			final ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();

			uploadedFiles = FileUploadHandler.getMultipartFileUploads(ctx);

			if (!untypedResponseMessageHeaders.acceptsFileUploads() && !uploadedFiles.getUploadedFiles().isEmpty()) {
				throw new RestHandlerException("File uploads not allowed.", HttpResponseStatus.BAD_REQUEST);
			}

			R request;
			if (msgContent.capacity() == 0) {
				try {
					request = MAPPER.readValue("{}", untypedResponseMessageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Request did not conform to expected format.", je);
					throw new RestHandlerException("Bad request received.", HttpResponseStatus.BAD_REQUEST, je);
				}
			} else {
				try {
					ByteBufInputStream in = new ByteBufInputStream(msgContent);
					request = MAPPER.readValue(in, untypedResponseMessageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Failed to read request.", je);
					throw new RestHandlerException(
						String.format("Request did not match expected format %s.", untypedResponseMessageHeaders.getRequestClass().getSimpleName()),
						HttpResponseStatus.BAD_REQUEST,
						je);
				}
			}

			final HandlerRequest<R, M> handlerRequest;

			try {
				handlerRequest = new HandlerRequest<>(request, untypedResponseMessageHeaders.getUnresolvedMessageParameters(), routed.pathParams(), routed.queryParams(), uploadedFiles.getUploadedFiles());
			} catch (HandlerRequestException hre) {
				log.error("Could not create the handler request.", hre);
				throw new RestHandlerException(
					String.format("Bad request, could not parse parameters: %s", hre.getMessage()),
					HttpResponseStatus.BAD_REQUEST,
					hre);
			}

			log.trace("Starting request processing.");
			CompletableFuture<Void> requestProcessingFuture = respondToRequest(
				ctx,
				httpRequest,
				handlerRequest,
				gateway);

			final FileUploads finalUploadedFiles = uploadedFiles;
			requestProcessingFuture
				.whenComplete((Void ignored, Throwable throwable) -> cleanupFileUploads(finalUploadedFiles));
		} catch (RestHandlerException rhe) {
			HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody(rhe.getMessage()),
				rhe.getHttpResponseStatus(),
				responseHeaders);
			cleanupFileUploads(uploadedFiles);
		} catch (Throwable e) {
			log.error("Request processing failed.", e);
			HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody("Internal server error."),
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				responseHeaders);
			cleanupFileUploads(uploadedFiles);
		}
	}

	private void cleanupFileUploads(@Nullable FileUploads uploadedFiles) {
		if (uploadedFiles != null) {
			try {
				uploadedFiles.close();
			} catch (IOException e) {
				log.warn("Could not cleanup uploaded files.", e);
			}
		}
	}

	/**
	 * Respond to the given {@link HandlerRequest}.
	 *
	 * @param ctx channel handler context to write the response
	 * @param httpRequest original http request
	 * @param handlerRequest typed handler request
	 * @param gateway leader gateway
	 * @return Future which is completed once the request has been processed
	 * @throws RestHandlerException if an exception occurred while responding
	 */
	protected abstract CompletableFuture<Void> respondToRequest(
		ChannelHandlerContext ctx,
		HttpRequest httpRequest,
		HandlerRequest<R, M> handlerRequest,
		T gateway) throws RestHandlerException;
}
