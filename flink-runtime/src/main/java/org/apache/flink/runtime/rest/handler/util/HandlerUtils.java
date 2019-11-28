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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Utilities for the REST handlers.
 */
public class HandlerUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HandlerUtils.class);

	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	/**
	 * Sends the given response and status code to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param httpRequest originating http request
	 * @param response which should be sent
	 * @param statusCode of the message to send
	 * @param headers additional header values
	 * @param <P> type of the response
	 */
	public static <P extends ResponseBody> CompletableFuture<Void> sendResponse(
			ChannelHandlerContext channelHandlerContext,
			HttpRequest httpRequest,
			P response,
			HttpResponseStatus statusCode,
			Map<String, String> headers) {
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, response);
		} catch (IOException ioe) {
			LOG.error("Internal server error. Could not map response to JSON.", ioe);
			return sendErrorResponse(
				channelHandlerContext,
				httpRequest,
				new ErrorResponseBody("Internal server error. Could not map response to JSON."),
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				headers);
		}
		return sendResponse(
			channelHandlerContext,
			httpRequest,
			sw.toString(),
			statusCode,
			headers);
	}

	/**
	 * Sends the given error response and status code to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param httpRequest originating http request
	 * @param errorMessage which should be sent
	 * @param statusCode of the message to send
	 * @param headers additional header values
	 */
	public static CompletableFuture<Void> sendErrorResponse(
			ChannelHandlerContext channelHandlerContext,
			HttpRequest httpRequest,
			ErrorResponseBody errorMessage,
			HttpResponseStatus statusCode,
			Map<String, String> headers) {

		return sendErrorResponse(
			channelHandlerContext,
			HttpHeaders.isKeepAlive(httpRequest),
			errorMessage,
			statusCode,
			headers);
	}

	/**
	 * Sends the given error response and status code to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param keepAlive If the connection should be kept alive.
	 * @param errorMessage which should be sent
	 * @param statusCode of the message to send
	 * @param headers additional header values
	 */
	public static CompletableFuture<Void> sendErrorResponse(
			ChannelHandlerContext channelHandlerContext,
			boolean keepAlive,
			ErrorResponseBody errorMessage,
			HttpResponseStatus statusCode,
			Map<String, String> headers) {

		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, errorMessage);
		} catch (IOException e) {
			// this should never happen
			LOG.error("Internal server error. Could not map error response to JSON.", e);
			return sendResponse(
				channelHandlerContext,
				keepAlive,
				"Internal server error. Could not map error response to JSON.",
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				headers);
		}
		return sendResponse(
			channelHandlerContext,
			keepAlive,
			sw.toString(),
			statusCode,
			headers);
	}

	/**
	 * Sends the given response and status code to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param httpRequest originating http request
	 * @param message which should be sent
	 * @param statusCode of the message to send
	 * @param headers additional header values
	 */
	public static CompletableFuture<Void> sendResponse(
			@Nonnull ChannelHandlerContext channelHandlerContext,
			@Nonnull HttpRequest httpRequest,
			@Nonnull String message,
			@Nonnull HttpResponseStatus statusCode,
			@Nonnull Map<String, String> headers) {

		return sendResponse(
			channelHandlerContext,
			HttpHeaders.isKeepAlive(httpRequest),
			message,
			statusCode,
			headers);
	}

	/**
	 * Sends the given response and status code to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param keepAlive If the connection should be kept alive.
	 * @param message which should be sent
	 * @param statusCode of the message to send
	 * @param headers additional header values
	 */
	public static CompletableFuture<Void> sendResponse(
			@Nonnull ChannelHandlerContext channelHandlerContext,
			boolean keepAlive,
			@Nonnull String message,
			@Nonnull HttpResponseStatus statusCode,
			@Nonnull Map<String, String> headers) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);

		response.headers().set(CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);

		for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
			response.headers().set(headerEntry.getKey(), headerEntry.getValue());
		}

		if (keepAlive) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}

		byte[] buf = message.getBytes(ConfigConstants.DEFAULT_CHARSET);
		ByteBuf b = Unpooled.copiedBuffer(buf);
		HttpHeaders.setContentLength(response, buf.length);

		// write the initial line and the header.
		channelHandlerContext.write(response);

		channelHandlerContext.write(b);

		ChannelFuture lastContentFuture = channelHandlerContext.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

		// close the connection, if no keep-alive is needed
		if (!keepAlive) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}

		return toCompletableFuture(lastContentFuture);
	}

	private static CompletableFuture<Void> toCompletableFuture(final ChannelFuture channelFuture) {
		final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
		channelFuture.addListener(future -> {
			if (future.isSuccess()) {
				completableFuture.complete(null);
			} else {
				completableFuture.completeExceptionally(future.cause());
			}
		});
		return completableFuture;
	}
}
