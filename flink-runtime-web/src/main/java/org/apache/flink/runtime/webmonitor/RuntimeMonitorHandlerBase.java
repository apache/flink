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
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.retriever.JobManagerRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.KeepAliveWrite;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Netty channel handler that processes all HTTP requests.
 * This handler takes the path parameters and delegates the work to a {@link RequestHandler}.
 * This handler also deals with setting correct response MIME types and returning
 * proper codes, like OK, NOT_FOUND, or SERVER_ERROR.
 */
@ChannelHandler.Sharable
public abstract class RuntimeMonitorHandlerBase extends SimpleChannelInboundHandler<Routed> {

	private final JobManagerRetriever retriever;

	protected final CompletableFuture<String> localJobManagerAddressFuture;

	protected final Time timeout;

	/** Whether the web service has https enabled. */
	protected final boolean httpsEnabled;

	protected String localJobManagerAddress;

	public RuntimeMonitorHandlerBase(
		JobManagerRetriever retriever,
		CompletableFuture<String> localJobManagerAddressFuture,
		Time timeout,
		boolean httpsEnabled) {

		this.retriever = checkNotNull(retriever);
		this.localJobManagerAddressFuture = checkNotNull(localJobManagerAddressFuture);
		this.timeout = checkNotNull(timeout);
		this.httpsEnabled = httpsEnabled;
	}

	/**
	 * Returns an array of REST URL's under which this handler can be registered.
	 *
	 * @return array containing REST URL's under which this handler can be registered.
	 */
	public abstract String[] getPaths();

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (localJobManagerAddressFuture.isDone()) {
			if (localJobManagerAddress == null) {
				localJobManagerAddress = localJobManagerAddressFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			}

			Optional<JobManagerGateway> optJobManagerGateway = retriever.getJobManagerGatewayNow();

			if (optJobManagerGateway.isPresent()) {
				JobManagerGateway jobManagerGateway = optJobManagerGateway.get();
				String redirectAddress = HandlerRedirectUtils.getRedirectAddress(
					localJobManagerAddress, jobManagerGateway, timeout);

				if (redirectAddress != null) {
					HttpResponse redirect = HandlerRedirectUtils.getRedirectResponse(redirectAddress, routed.path(),
						httpsEnabled);
					KeepAliveWrite.flush(ctx, routed.request(), redirect);
				}
				else {
					respondAsLeader(ctx, routed, jobManagerGateway);
				}
			} else {
				KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
			}
		} else {
			KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
		}
	}

	protected abstract void respondAsLeader(ChannelHandlerContext ctx, Routed routed, JobManagerGateway jobManagerGateway);
}
