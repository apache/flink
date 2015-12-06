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

package org.apache.flink.runtime.webmonitor.handlers;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;

import org.apache.flink.runtime.webmonitor.files.MimeTypes;

import java.io.UnsupportedEncodingException;

/**
 * Responder that returns a constant String.
 */
@ChannelHandler.Sharable
public class ConstantTextHandler extends SimpleChannelInboundHandler<Routed> {
	
	private static final String MIME_TYPE = MimeTypes.getMimeTypeForExtension("txt");
	
	private final byte[] encodedText;
	
	public ConstantTextHandler(String text) {
		try {
			this.encodedText = text.getBytes("UTF-8");
		}
		catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		HttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(encodedText));

		response.headers().set(HttpHeaders.Names.CONTENT_ENCODING, "utf-8");
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, encodedText.length);
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, MIME_TYPE);

		KeepAliveWrite.flush(ctx, routed.request(), response);
	}
}
