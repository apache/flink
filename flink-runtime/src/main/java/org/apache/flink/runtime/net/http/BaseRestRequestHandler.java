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

package org.apache.flink.runtime.net.http;

import com.google.common.net.MediaType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * An abstract REST request handler.
 *
 * Provides a request-response abstraction for the implementing class.
 */
public abstract class BaseRestRequestHandler<T,OUT>
	extends SimpleChannelInboundHandler<RestRequest<T>>
	implements RestResponseHandler<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseRestRequestHandler.class);

	private final Class<OUT> responseType;
	private long responseTimeout;
	private TimeUnit unit;

	public BaseRestRequestHandler(Class<OUT> responseType) {
		this(responseType, 0, TimeUnit.SECONDS);
	}

	public BaseRestRequestHandler(Class<OUT> responseType, long responseTimeout, TimeUnit unit) {
		this.responseType = responseType;
		this.responseTimeout = responseTimeout;
		this.unit = unit;
	}

	@Override
	public Class<OUT> getContentType() {
		return responseType;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, final RestRequest<T> request) throws Exception {

		final Channel channel = ctx.channel();

		if(LOG.isDebugEnabled()) {
			LOG.debug(blockquote(request.toString()));
		}

		// validate the request
		boolean valid = true;
		if(!request.headers().contains(HttpHeaders.Names.ACCEPT)) {
			valid = false;
		}
		else {
			MediaType acceptType = MediaType.parse(request.headers().get(HttpHeaders.Names.ACCEPT));
			if(!MediaType.JSON_UTF_8.is(acceptType)) {
				valid = false;
			}
		}

		if(!valid) {
			RestResponse response = new DefaultRestResponse(request.getProtocolVersion(), HttpResponseStatus.NOT_ACCEPTABLE);
			writeResponse(channel, request, response);
			return;
		}

		// handle
		final Future<RestResponse<OUT>> responseFuture = handleRequest(ctx, request);

		// produce a response
		ReferenceCountUtil.retain(request);
		responseFuture.addListener(new GenericFutureListener<Future<RestResponse>>() {
			@Override
			public void operationComplete(Future<RestResponse> future) throws Exception {
				try {
					if(future.isCancelled()) {
						return;
					}

					RestResponse response;
					if(future.isSuccess()) {
						response = future.getNow();
					}
					else {
						LOG.error("Error occurred in processing HTTP request", future.cause());
						response = new DefaultRestResponse<>(
							request.getProtocolVersion(), HttpResponseStatus.INTERNAL_SERVER_ERROR, (T) null);
					}
					writeResponse(channel, request, response);
				}
				finally {
					ReferenceCountUtil.release(request);
				}
			}
		});

		if(responseTimeout > 0 && responseFuture.isCancellable() && !responseFuture.isDone()) {
			LOG.debug("Scheduling a timeout callback.");
			ReferenceCountUtil.retain(request);
			ctx.executor().schedule(new Runnable() {
				@Override
				public void run() {
					try {
						if (responseFuture.isDone()) {
							return;
						}

						if (responseFuture.cancel(false)) {
							LOG.warn("Took too long to generate a response.");
							RestResponse response = new DefaultRestResponse<>(
								request.getProtocolVersion(), HttpResponseStatus.SERVICE_UNAVAILABLE, (T) null);
							writeResponse(channel, request, response);
						}
					}
					finally {
						ReferenceCountUtil.release(request);
					}
				}
			}, responseTimeout, unit);
		}
	}

	private void writeResponse(Channel channel, RestRequest<T> request, RestResponse<?> response) {
		if(LOG.isDebugEnabled()) {
			LOG.debug(blockquote(response.toString()));
		}

		KeepAliveWrite
			.flush(channel, request, response)
			.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
	}

	/**
	 * Handle the REST request to produce a response.
	 * @param ctx the channel handler context.
	 * @param request the REST request.
     * @return the promise of a REST response.
     */
	protected abstract Future<RestResponse<OUT>> handleRequest(ChannelHandlerContext ctx, RestRequest<T> request);

	// Utilities ------------------------------------------------

	/**
	 * Create a 400 "Bad Request" response.
     */
	protected RestResponse badRequest(RestRequest request) {
		return new DefaultRestResponse(request.getProtocolVersion(), HttpResponseStatus.BAD_REQUEST);
	}

	/**
	 * Create a 404 "Not Found" response.
	 * @param request
	 * @return
     */
	protected RestResponse notFound(RestRequest request) {
		return new DefaultRestResponse(request.getProtocolVersion(), HttpResponseStatus.NOT_FOUND);
	}

	private static String blockquote(String str) {
		return String.join("\n> ", str.split("\n"));
	}

}
