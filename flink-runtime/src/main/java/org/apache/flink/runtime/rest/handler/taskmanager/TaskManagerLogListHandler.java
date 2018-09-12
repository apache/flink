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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.handler.util.KeepAliveWrite;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.util.JsonUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import javax.annotation.Nonnull;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * TaskManagerLogListHandler serves the request which gets the historical log file list of a given taskmanager.
 */
public class TaskManagerLogListHandler extends AbstractHandler {
	private static final Charset ENCODING = Charset.forName("UTF-8");
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	public TaskManagerLogListHandler(
		@Nonnull CompletableFuture localAddressFuture,
		@Nonnull GatewayRetriever leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map responseHeaders,
		@Nonnull UntypedResponseMessageHeaders untypedResponseMessageHeaders,
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(localAddressFuture, leaderRetriever, timeout, responseHeaders, untypedResponseMessageHeaders);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
	}

	@Override
	public CompletableFuture<Void> respondToRequest(
		ChannelHandlerContext ctx,
		HttpRequest httpRequest,
		HandlerRequest handlerRequest,
		RestfulGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = (ResourceID) handlerRequest.getPathParameter(TaskManagerIdPathParameter.class);
		Optional<ResourceManagerGateway> resourceManagerGatewayOptional = resourceManagerGatewayRetriever.getNow();
		ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayOptional.orElseThrow(
			() -> new RestHandlerException("Cannot connect to ResourceManager right now. Please try to refresh.", HttpResponseStatus.NOT_FOUND));
		String body = "";
		try {
			String[] relatedFiles = resourceManagerGateway.requestTaskManagerLogList(taskManagerId, timeout).get();
			body = JsonUtils.createSortedFilesJson(relatedFiles);
		} catch (Exception e) {
			HandlerUtils.sendErrorResponse(
				ctx,
				httpRequest,
				new ErrorResponseBody("Log list not found."),
				HttpResponseStatus.NOT_FOUND,
				responseHeaders);
		}
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(body.getBytes()));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		KeepAliveWrite.flush(ctx, httpRequest, response);
		return CompletableFuture.completedFuture(null);
	}
}
