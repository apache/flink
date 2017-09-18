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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.KeepAliveWrite;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link SimpleChannelInboundHandler} which encapsulates the redirection logic for the
 * REST endpoints.
 *
 * @param <T> type of the leader to retrieve
 */
@ChannelHandler.Sharable
public abstract class RedirectHandler<T extends RestfulGateway> extends SimpleChannelInboundHandler<Routed> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private final CompletableFuture<String> localAddressFuture;

	protected final GatewayRetriever<T> leaderRetriever;

	protected final Time timeout;

	/** Whether the web service has https enabled. */
	protected final boolean httpsEnabled;

	private String localAddress;

	protected RedirectHandler(
			@Nonnull CompletableFuture<String> localAddressFuture,
			@Nonnull GatewayRetriever<T> leaderRetriever,
			@Nonnull Time timeout,
			boolean httpsEnabled) {
		this.localAddressFuture = Preconditions.checkNotNull(localAddressFuture);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.httpsEnabled = httpsEnabled;
		localAddress = null;
	}

	@Override
	protected void channelRead0(
		ChannelHandlerContext channelHandlerContext,
		Routed routed) throws Exception {

		if (localAddressFuture.isDone()) {
			if (localAddress == null) {
				try {
					localAddress = localAddressFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				} catch (Exception e) {
					logger.error("Could not obtain local address.", e);
					KeepAliveWrite.flush(channelHandlerContext, routed.request(), HandlerRedirectUtils.getErrorResponse(e));
				}
			}

			OptionalConsumer<T> optLeaderConsumer = OptionalConsumer.of(leaderRetriever.getNow());

			optLeaderConsumer.ifPresent(
				(T gateway) -> {
					OptionalConsumer<CompletableFuture<String>> optRedirectAddressConsumer = OptionalConsumer.of(
						HandlerRedirectUtils.getRedirectAddress(
							localAddress,
							gateway,
							timeout));

					optRedirectAddressConsumer
						.ifPresent(
							(CompletableFuture<String> redirectAddressFuture) ->
								redirectAddressFuture.whenComplete(
									(String redirectAddress, Throwable throwable) -> {
										HttpResponse response;

										if (throwable != null) {
											logger.error("Could not retrieve the redirect address.", throwable);
											response = HandlerRedirectUtils.getErrorResponse(throwable);
										} else {
											response = HandlerRedirectUtils.getRedirectResponse(
												redirectAddress,
												routed.path(),
												httpsEnabled);
										}

										KeepAliveWrite.flush(channelHandlerContext, routed.request(), response);
									}
								))
						.ifNotPresent(
							() -> {
								try {
									respondAsLeader(channelHandlerContext, routed, gateway);
								} catch (Exception e) {
									logger.error("Error while responding as leader.", e);
									KeepAliveWrite.flush(
										channelHandlerContext,
										routed.request(),
										HandlerRedirectUtils.getErrorResponse(e));
								}
							});
				}
			).ifNotPresent(
				() -> KeepAliveWrite.flush(channelHandlerContext, routed.request(), HandlerRedirectUtils.getUnavailableResponse()));
		} else {
			KeepAliveWrite.flush(channelHandlerContext, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
		}
	}

	protected abstract void respondAsLeader(ChannelHandlerContext channelHandlerContext, Routed routed, T gateway) throws Exception;
}
