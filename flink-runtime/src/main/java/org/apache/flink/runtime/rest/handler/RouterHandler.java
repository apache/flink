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

import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Handler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class is an extension of {@link Handler} that replaces the standard error response to be identical with those
 * sent by the {@link AbstractRestHandler}.
 */
public class RouterHandler extends Handler {

	private final Map<String, String> responseHeaders;

	public RouterHandler(Router router, final Map<String, String> responseHeaders) {
		super(router);
		this.responseHeaders = requireNonNull(responseHeaders);
	}

	@Override
	protected void respondNotFound(ChannelHandlerContext ctx, HttpRequest request) {
		HandlerUtils.sendErrorResponse(
			ctx,
			request,
			new ErrorResponseBody("Not found."),
			HttpResponseStatus.NOT_FOUND,
			responseHeaders);
	}
}
