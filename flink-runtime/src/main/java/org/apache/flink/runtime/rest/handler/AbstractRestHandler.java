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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}s and {@link ResponseBody}s.
 *
 * <p>Subclasses must be thread-safe.
 *
 * @param <R> type of incoming requests
 * @param <P> type of outgoing responses
 */
@ChannelHandler.Sharable
public abstract class AbstractRestHandler<T extends RestfulGateway, R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends RedirectHandler<T> {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	private final MessageHeaders<R, P, M> messageHeaders;

	protected AbstractRestHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends T> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<R, P, M> messageHeaders) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders);
		this.messageHeaders = messageHeaders;
	}

	public MessageHeaders<R, P, M> getMessageHeaders() {
		return messageHeaders;
	}

	@Override
	protected void respondAsLeader(final ChannelHandlerContext ctx, Routed routed, T gateway) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("Received request " + routed.request().getUri() + '.');
		}

		final HttpRequest httpRequest = routed.request();

		try {
			if (!(httpRequest instanceof FullHttpRequest)) {
				// The RestServerEndpoint defines a HttpObjectAggregator in the pipeline that always returns
				// FullHttpRequests.
				log.error("Implementation error: Received a request that wasn't a FullHttpRequest.");
				HandlerUtils.sendErrorResponse(
					ctx,
					httpRequest,
					new ErrorResponseBody("Bad request received."),
					HttpResponseStatus.BAD_REQUEST,
					responseHeaders);
				return;
			}

			ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();

			R request;
			if (msgContent.capacity() == 0) {
				try {
					request = mapper.readValue("{}", messageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Request did not conform to expected format.", je);
					HandlerUtils.sendErrorResponse(
						ctx,
						httpRequest,
						new ErrorResponseBody("Bad request received."),
						HttpResponseStatus.BAD_REQUEST,
						responseHeaders);
					return;
				}
			} else {
				try {
					ByteBufInputStream in = new ByteBufInputStream(msgContent);
					request = mapper.readValue(in, messageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Failed to read request.", je);
					HandlerUtils.sendErrorResponse(
						ctx,
						httpRequest,
						new ErrorResponseBody(String.format("Request did not match expected format %s.", messageHeaders.getRequestClass().getSimpleName())),
						HttpResponseStatus.BAD_REQUEST,
						responseHeaders);
					return;
				}
			}

			final HandlerRequest<R, M> handlerRequest;

			try {
				handlerRequest = new HandlerRequest<>(request, messageHeaders.getUnresolvedMessageParameters(), routed.pathParams(), routed.queryParams());
			} catch (HandlerRequestException hre) {
				log.error("Could not create the handler request.", hre);

				HandlerUtils.sendErrorResponse(
					ctx,
					httpRequest,
					new ErrorResponseBody(String.format("Bad request, could not parse parameters: %s", hre.getMessage())),
					HttpResponseStatus.BAD_REQUEST,
					responseHeaders);
				return;
			}

			CompletableFuture<P> response;

			try {
				response = handleRequest(handlerRequest, gateway);
			} catch (RestHandlerException e) {
				response = FutureUtils.completedExceptionally(e);
			}

			response.whenComplete((P resp, Throwable throwable) -> {
				if (throwable != null) {

					Throwable error = ExceptionUtils.stripCompletionException(throwable);

					if (error instanceof RestHandlerException) {
						final RestHandlerException rhe = (RestHandlerException) error;

						log.error("Exception occurred in REST handler.", error);

						HandlerUtils.sendErrorResponse(
							ctx,
							httpRequest,
							new ErrorResponseBody(rhe.getMessage()),
							rhe.getHttpResponseStatus(),
							responseHeaders);
					} else {
						log.error("Implementation error: Unhandled exception.", error);
						HandlerUtils.sendErrorResponse(
							ctx,
							httpRequest,
							new ErrorResponseBody("Internal server error."),
							HttpResponseStatus.INTERNAL_SERVER_ERROR,
							responseHeaders);
					}
				} else {
					HandlerUtils.sendResponse(
						ctx,
						httpRequest,
						resp,
						messageHeaders.getResponseStatusCode(),
						responseHeaders);
				}
			});
		} catch (Throwable e) {
			log.error("Request processing failed.", e);
			HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody("Internal server error."),
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				responseHeaders);
		}
	}

	/**
	 * This method is called for every incoming request and returns a {@link CompletableFuture} containing a the response.
	 *
	 * <p>Implementations may decide whether to throw {@link RestHandlerException}s or fail the returned
	 * {@link CompletableFuture} with a {@link RestHandlerException}.
	 *
	 * <p>Failing the future with another exception type or throwing unchecked exceptions is regarded as an
	 * implementation error as it does not allow us to provide a meaningful HTTP status code. In this case a
	 * {@link HttpResponseStatus#INTERNAL_SERVER_ERROR} will be returned.
	 *
	 * @param request request that should be handled
	 * @param gateway leader gateway
	 * @return future containing a handler response
	 * @throws RestHandlerException if the handling failed
	 */
	protected abstract CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R, M> request, @Nonnull T gateway) throws RestHandlerException;
}
