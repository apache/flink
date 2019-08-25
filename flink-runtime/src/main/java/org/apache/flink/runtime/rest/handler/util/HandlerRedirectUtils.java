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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.configuration.ConfigConstants;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import javax.annotation.Nullable;

import java.nio.charset.Charset;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities to extract a redirect address.
 *
 * <p>This is necessary at the moment, because many execution graph structures are not serializable.
 * The proper solution here is to have these serializable and transparently work with the leading
 * job manager instead of redirecting.
 */
public class HandlerRedirectUtils {

	public static final Charset ENCODING = ConfigConstants.DEFAULT_CHARSET;

	public static HttpResponse getRedirectResponse(String redirectAddress, String path, HttpResponseStatus code) {
		checkNotNull(redirectAddress, "Redirect address");
		checkNotNull(path, "Path");

		String newLocation = String.format("%s%s", redirectAddress, path);

		HttpResponse redirectResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, code);
		redirectResponse.headers().set(HttpHeaders.Names.LOCATION, newLocation);
		redirectResponse.headers().set(HttpHeaders.Names.CONTENT_LENGTH, 0);

		return redirectResponse;
	}

	public static HttpResponse getResponse(HttpResponseStatus status, @Nullable String message) {
		ByteBuf messageByteBuf = message == null ? Unpooled.buffer(0)
			: Unpooled.wrappedBuffer(message.getBytes(ENCODING));
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, messageByteBuf);
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=" + ENCODING.name());
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());

		return response;
	}
}
