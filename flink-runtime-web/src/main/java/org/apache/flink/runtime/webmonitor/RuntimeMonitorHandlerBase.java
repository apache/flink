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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The Netty channel handler that processes all HTTP requests.
 * This handler takes the path parameters and delegates the work to a {@link RequestHandler}.
 * This handler also deals with setting correct response MIME types and returning
 * proper codes, like OK, NOT_FOUND, or SERVER_ERROR.
 */
@ChannelHandler.Sharable
public abstract class RuntimeMonitorHandlerBase extends SimpleChannelInboundHandler<Routed> {

	private final JobManagerRetriever retriever;

	protected final Future<String> localJobManagerAddressFuture;

	protected final FiniteDuration timeout;

	protected String localJobManagerAddress;
	
	public RuntimeMonitorHandlerBase(
		JobManagerRetriever retriever,
		Future<String> localJobManagerAddressFuture,
		FiniteDuration timeout) {

		this.retriever = checkNotNull(retriever);
		this.localJobManagerAddressFuture = checkNotNull(localJobManagerAddressFuture);
		this.timeout = checkNotNull(timeout);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (localJobManagerAddressFuture.isCompleted()) {
			if (localJobManagerAddress == null) {
				localJobManagerAddress = Await.result(localJobManagerAddressFuture, timeout);
			}

			Option<Tuple2<ActorGateway, Integer>> jobManager = retriever.getJobManagerGatewayAndWebPort();

			if (jobManager.isDefined()) {
				Tuple2<ActorGateway, Integer> gatewayPort = jobManager.get();
				String redirectAddress = HandlerRedirectUtils.getRedirectAddress(
					localJobManagerAddress, gatewayPort);

				if (redirectAddress != null) {
					HttpResponse redirect = HandlerRedirectUtils.getRedirectResponse(redirectAddress, routed.path());
					KeepAliveWrite.flush(ctx, routed.request(), redirect);
				}
				else {
					respondAsLeader(ctx, routed, gatewayPort._1());
				}
			} else {
				KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
			}
		} else {
			KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
		}
	}

	protected abstract void respondAsLeader(ChannelHandlerContext ctx, Routed routed, ActorGateway jobManager);
}
