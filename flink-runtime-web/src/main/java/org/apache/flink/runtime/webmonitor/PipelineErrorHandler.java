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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;

/**
 * This is the last handler in the pipeline. It logs all error messages and sends exception
 * responses.
 */
@ChannelHandler.Sharable
public class PipelineErrorHandler extends SimpleChannelInboundHandler<Object> {

	/** The logger to which the handler writes the log statements. */
	private final Logger logger;

	public PipelineErrorHandler(Logger logger) {
		this.logger = logger;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object message) {
		// we can't deal with this message. No one in the pipeline handled it. Log it.
		logger.debug("Unknown message received: {}", message);
		sendError(ctx, "Unknown message received.");
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.debug("Unhandled exception.", cause);
		sendError(ctx, ExceptionUtils.stringifyException(cause));
	}

	private void sendError(ChannelHandlerContext ctx, String error) {
		if (ctx.channel().isActive()) {
			DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
				HttpResponseStatus.INTERNAL_SERVER_ERROR,
				Unpooled.wrappedBuffer(error.getBytes(ConfigConstants.DEFAULT_CHARSET)));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
			response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

			ctx.writeAndFlush(response);
		}
	}
}
