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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;
import org.apache.flink.runtime.webmonitor.handlers.RequestHandler;
import org.apache.flink.util.ExceptionUtils;

import java.nio.charset.Charset;

/**
 * The Netty channel handler that processes all HTTP requests.
 * This handler takes the path parameters and delegates the work to a {@link RequestHandler}.
 * This handler also deals with setting correct response MIME types and returning
 * proper codes, like OK, NOT_FOUND, or SERVER_ERROR.
 */
@ChannelHandler.Sharable
public class RuntimeMonitorHandler extends SimpleChannelInboundHandler<Routed> {
	
	private static final Charset ENCODING = Charset.forName("UTF-8");
	
	private final RequestHandler handler;
	
	private final String contentType;
	
	public RuntimeMonitorHandler(RequestHandler handler) {
		if (handler == null) {
			throw new NullPointerException();
		}
		this.handler = handler;
		this.contentType = (handler instanceof RequestHandler.JsonResponse) ? "application/json" : "text/plain";
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		DefaultFullHttpResponse response;
		
		try {
			String result = handler.handleRequest(routed.pathParams());
			byte[] bytes = result.getBytes(ENCODING);
			
			response = new DefaultFullHttpResponse(
					HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, contentType);
		}
		catch (NotFoundException e) {
			// this should result in a 404 error code (not found)
			ByteBuf message = e.getMessage() == null ? Unpooled.buffer(0)
					: Unpooled.wrappedBuffer(e.getMessage().getBytes(ENCODING));
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, message);
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		}
		catch (Exception e) {
			byte[] bytes = ExceptionUtils.stringifyException(e).getBytes(ENCODING);
			response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
					HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.wrappedBuffer(bytes));
			response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");
		}

		response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		KeepAliveWrite.flush(ctx, routed.request(), response);
	}
}
