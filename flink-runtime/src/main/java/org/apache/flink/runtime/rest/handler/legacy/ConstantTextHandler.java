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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.KeepAliveWrite;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

/**
 * Responder that returns a constant String.
 */
@ChannelHandler.Sharable
public class ConstantTextHandler extends SimpleChannelInboundHandler<RoutedRequest> {

	private final byte[] encodedText;

	public ConstantTextHandler(String text) {
		this.encodedText = text.getBytes(ConfigConstants.DEFAULT_CHARSET);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, RoutedRequest routed) throws Exception {
		HttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(encodedText));

		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, encodedText.length);
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain");

		KeepAliveWrite.flush(ctx, routed.getRequest(), response);
	}
}
