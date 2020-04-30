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
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Map;

/**
 * {@link SimpleChannelInboundHandler} which encapsulates the leader retrieval logic for the
 * REST endpoints.
 *
 * @param <T> type of the leader to retrieve
 */
@ChannelHandler.Sharable
public abstract class LeaderRetrievalHandler<T extends RestfulGateway> extends SimpleChannelInboundHandler<RoutedRequest> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final GatewayRetriever<? extends T> leaderRetriever;

	protected final Time timeout;

	protected final Map<String, String> responseHeaders;

	protected LeaderRetrievalHandler(
			@Nonnull GatewayRetriever<? extends T> leaderRetriever,
			@Nonnull Time timeout,
			@Nonnull Map<String, String> responseHeaders) {
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.responseHeaders = Preconditions.checkNotNull(responseHeaders);
	}

	@Override
	protected void channelRead0(
		ChannelHandlerContext channelHandlerContext,
		RoutedRequest routedRequest) {

		HttpRequest request = routedRequest.getRequest();

		OptionalConsumer<? extends T> optLeaderConsumer = OptionalConsumer.of(leaderRetriever.getNow());

		optLeaderConsumer.ifPresent(
			gateway -> {
				try {
					respondAsLeader(channelHandlerContext, routedRequest, gateway);
				} catch (Exception e) {
					logger.error("Error while responding to the http request.", e);
					HandlerUtils.sendErrorResponse(
						channelHandlerContext,
						request,
						new ErrorResponseBody("Error while responding to the http request."),
						HttpResponseStatus.INTERNAL_SERVER_ERROR,
						responseHeaders);
				}
			}
		).ifNotPresent(
			() ->
				HandlerUtils.sendErrorResponse(
					channelHandlerContext,
					request,
					new ErrorResponseBody("Service temporarily unavailable due to an ongoing leader election. Please refresh."),
					HttpResponseStatus.SERVICE_UNAVAILABLE,
					responseHeaders));
	}

	protected abstract void respondAsLeader(ChannelHandlerContext channelHandlerContext, RoutedRequest request, T gateway) throws Exception;
}
