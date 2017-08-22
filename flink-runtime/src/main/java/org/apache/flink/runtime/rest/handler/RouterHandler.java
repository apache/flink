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

import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Handler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an extension of {@link Handler} that replaces the standard error response to be identical with those
 * sent by the {@link AbstractRestHandler}.
 */
public class RouterHandler extends Handler {
	private static final Logger LOG = LoggerFactory.getLogger(RouterHandler.class);

	public RouterHandler(Router router) {
		super(router);
	}

	@Override
	protected void respondNotFound(ChannelHandlerContext ctx, HttpRequest request) {
		AbstractRestHandler.sendErrorResponse(new ErrorResponseBody("Not found."), HttpResponseStatus.NOT_FOUND, ctx, request);
	}
}
