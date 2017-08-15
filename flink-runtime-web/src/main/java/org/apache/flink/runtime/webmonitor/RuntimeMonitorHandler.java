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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.KeepAliveWrite;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.webmonitor.handlers.HandlerRedirectUtils.ENCODING;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Netty channel handler that processes all HTTP requests.
 * This handler takes the path parameters and delegates the work to a {@link RequestHandler}.
 * This handler also deals with setting correct response MIME types and returning
 * proper codes, like OK, NOT_FOUND, or SERVER_ERROR.
 */
@ChannelHandler.Sharable
public class RuntimeMonitorHandler extends RuntimeMonitorHandlerBase {

	private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorHandler.class);

	public static final String WEB_MONITOR_ADDRESS_KEY = "web.monitor.address";

	private final RequestHandler handler;

	private final String allowOrigin;

	public RuntimeMonitorHandler(
			WebMonitorConfig cfg,
			RequestHandler handler,
			GatewayRetriever<JobManagerGateway> retriever,
			CompletableFuture<String> localJobManagerAddressFuture,
			Time timeout,
			boolean httpsEnabled) {

		super(retriever, localJobManagerAddressFuture, timeout, httpsEnabled);
		this.handler = checkNotNull(handler);
		this.allowOrigin = cfg.getAllowOrigin();
	}

	@Override
	public String[] getPaths() {
		return handler.getPaths();
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext ctx, Routed routed, JobManagerGateway jobManagerGateway) {
		CompletableFuture<FullHttpResponse> responseFuture;

		try {
			// we only pass the first element in the list to the handlers.
			Map<String, String> queryParams = new HashMap<>();
			for (String key : routed.queryParams().keySet()) {
				queryParams.put(key, routed.queryParam(key));
			}

			Map<String, String> pathParams = new HashMap<>(routed.pathParams().size());
			for (String key : routed.pathParams().keySet()) {
				pathParams.put(key, URLDecoder.decode(routed.pathParams().get(key), ENCODING.toString()));
			}

			InetSocketAddress address = (InetSocketAddress) ctx.channel().localAddress();
			queryParams.put(WEB_MONITOR_ADDRESS_KEY,
				(httpsEnabled ? "https://" : "http://") + address.getHostName() + ":" + address.getPort());

			responseFuture = handler.handleRequest(pathParams, queryParams, jobManagerGateway);
		} catch (Exception e) {
			responseFuture = FutureUtils.completedExceptionally(e);
		}

		responseFuture.whenComplete(
			(FullHttpResponse httpResponse, Throwable throwable) -> {
				final FullHttpResponse finalResponse;

				if (throwable != null) {
					LOG.debug("Error while handling request.", throwable);

					Optional<Throwable> optNotFound = ExceptionUtils.findThrowable(throwable, NotFoundException.class);

					if (optNotFound.isPresent()) {
						// this should result in a 404 error code (not found)
						finalResponse = HandlerRedirectUtils.getResponse(HttpResponseStatus.NOT_FOUND, optNotFound.get().getMessage());
					} else {
						finalResponse = HandlerRedirectUtils.getErrorResponse(throwable);
					}
				} else {
					finalResponse = httpResponse;
				}

				finalResponse.headers().set(HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN, allowOrigin);
				KeepAliveWrite.flush(ctx, routed.request(), finalResponse);
			});
	}
}
