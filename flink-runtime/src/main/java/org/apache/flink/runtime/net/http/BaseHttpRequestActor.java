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

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.util.ReferenceCountUtil;
import scala.Option;

/**
 * An actor that handles a single HTTP request.
 *
 * Provides the following convenience functionality:
 *  - stops itself after an HTTP response is sent
 *  - ReceiveTimeout produces a 503 (Service Unavailable) response
 *  - unhandled exceptions produce a 500 (Internal Server Error) response
 */
public class BaseHttpRequestActor extends UntypedActor {

	private final Channel channel;
	private final HttpRequest request;

	public BaseHttpRequestActor(Channel channel, HttpRequest request) {
		this.channel = channel;
		this.request = request;
	}

	public Channel channel() {
		return channel;
	}

	public HttpRequest request() {
		return request;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof ReceiveTimeout) {
			sendResponse(new DefaultFullHttpResponse(
				request.getProtocolVersion(), HttpResponseStatus.SERVICE_UNAVAILABLE), true);
		}
		else {
			unhandled(message);
		}
	}

	@Override
	public void preRestart(Throwable reason, Option<Object> message) throws Exception {
		if(channel.isActive()) {
			sendResponse(new DefaultFullHttpResponse(
				request.getProtocolVersion(), HttpResponseStatus.INTERNAL_SERVER_ERROR), false);
		}
		super.preRestart(reason, message);
	}

	@Override
	public void postStop() throws Exception {
		ReferenceCountUtil.release(request);
		super.postStop();
	}

	// utilities --------------------------------------------------

	protected void sendBadRequest() {
		sendResponse(new DefaultFullHttpResponse(
			request.getProtocolVersion(), HttpResponseStatus.BAD_REQUEST), true);
	}


	protected void sendNotFound() {
		sendResponse(new DefaultFullHttpResponse(
			request.getProtocolVersion(), HttpResponseStatus.NOT_FOUND), true);
	}

	/**
	 * Send an HTTP response and optionally stop the actor.
	 * @param response the response to send.
	 * @param stop indicates whether to stop.
     */
	protected void sendResponse(HttpResponse response, boolean stop) {
		KeepAliveWrite
			.flush(channel, request, response)
			.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

		if(stop) {
			context().stop(self());
		}
	}
}
