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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}s and {@link ResponseBody}s.
 *
 * <p>Subclasses must be thread-safe.
 *
 * @param <R> type of incoming requests
 * @param <P> type of outgoing responses
 */
@ChannelHandler.Sharable
public abstract class AbstractRestHandler<R extends RequestBody, P extends ResponseBody, M extends MessageParameters> extends SimpleChannelInboundHandler<Routed> {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	private final MessageHeaders<R, P, M> messageHeaders;

	protected AbstractRestHandler(MessageHeaders<R, P, M> messageHeaders) {
		this.messageHeaders = messageHeaders;
	}

	public MessageHeaders<R, P, M> getMessageHeaders() {
		return messageHeaders;
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("Received request " + routed.request().getUri() + '.');
		}

		final HttpRequest httpRequest = routed.request();

		try {
			if (!(httpRequest instanceof FullHttpRequest)) {
				// The RestServerEndpoint defines a HttpObjectAggregator in the pipeline that always returns
				// FullHttpRequests.
				log.error("Implementation error: Received a request that wasn't a FullHttpRequest.");
				sendErrorResponse(new ErrorResponseBody("Bad request received."), HttpResponseStatus.BAD_REQUEST, ctx, httpRequest);
				return;
			}

			ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();

			R request;
			if (msgContent.capacity() == 0) {
				try {
					request = mapper.readValue("{}", messageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Implementation error: Get request bodies must have a no-argument constructor.", je);
					sendErrorResponse(new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
					return;
				}
			} else {
				try {
					ByteBufInputStream in = new ByteBufInputStream(msgContent);
					request = mapper.readValue(in, messageHeaders.getRequestClass());
				} catch (JsonParseException | JsonMappingException je) {
					log.error("Failed to read request.", je);
					sendErrorResponse(new ErrorResponseBody(String.format("Request did not match expected format %s.", messageHeaders.getRequestClass().getSimpleName())), HttpResponseStatus.BAD_REQUEST, ctx, httpRequest);
					return;
				}
			}

			CompletableFuture<P> response;
			try {
				HandlerRequest<R, M> handlerRequest = new HandlerRequest<>(request, messageHeaders.getUnresolvedMessageParameters(), routed.pathParams(), routed.queryParams());
				response = handleRequest(handlerRequest);
			} catch (Exception e) {
				response = FutureUtils.completedExceptionally(e);
			}

			response.whenComplete((P resp, Throwable error) -> {
				if (error != null) {
					if (error instanceof RestHandlerException) {
						RestHandlerException rhe = (RestHandlerException) error;
						sendErrorResponse(new ErrorResponseBody(rhe.getErrorMessage()), rhe.getErrorCode(), ctx, httpRequest);
					} else {
						log.error("Implementation error: Unhandled exception.", error);
						sendErrorResponse(new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
					}
				} else {
					sendResponse(messageHeaders.getResponseStatusCode(), resp, ctx, httpRequest);
				}
			});
		} catch (Exception e) {
			log.error("Request processing failed.", e);
			sendErrorResponse(new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
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
	 * @return future containing a handler response
	 * @throws RestHandlerException if the handling failed
	 */
	protected abstract CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R, M> request) throws RestHandlerException;

	private static <P extends ResponseBody> void sendResponse(HttpResponseStatus statusCode, P response, ChannelHandlerContext ctx, HttpRequest httpRequest) {
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, response);
		} catch (IOException ioe) {
			sendErrorResponse(new ErrorResponseBody("Internal server error. Could not map response to JSON."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
			return;
		}
		sendResponse(ctx, httpRequest, statusCode, sw.toString());
	}

	static void sendErrorResponse(ErrorResponseBody error, HttpResponseStatus statusCode, ChannelHandlerContext ctx, HttpRequest httpRequest) {

		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, error);
		} catch (IOException e) {
			// this should never happen
			sendResponse(ctx, httpRequest, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error. Could not map error response to JSON.");
		}
		sendResponse(ctx, httpRequest, statusCode, sw.toString());
	}

	private static void sendResponse(@Nonnull ChannelHandlerContext ctx, @Nonnull HttpRequest httpRequest, @Nonnull HttpResponseStatus statusCode, @Nonnull String message) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);

		response.headers().set(CONTENT_TYPE, "application/json");

		if (HttpHeaders.isKeepAlive(httpRequest)) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}

		byte[] buf = message.getBytes(ConfigConstants.DEFAULT_CHARSET);
		ByteBuf b = Unpooled.copiedBuffer(buf);
		HttpHeaders.setContentLength(response, buf.length);

		// write the initial line and the header.
		ctx.write(response);

		ctx.write(b);

		ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

		// close the connection, if no keep-alive is needed
		if (!HttpHeaders.isKeepAlive(httpRequest)) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}
	}
}
