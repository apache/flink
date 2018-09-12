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

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.KeepAliveWrite;
import org.apache.flink.runtime.util.JsonUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import javax.annotation.Nonnull;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * LogListHandler serves the request which gets the historical log file list of jobmanager.
 */
public class LogListHandler extends RedirectHandler {
	private static final Charset ENCODING = Charset.forName("UTF-8");
	private final File parentPath;
	public LogListHandler(
		@Nonnull CompletableFuture localAddressFuture,
		@Nonnull GatewayRetriever leaderRetriever,
		@Nonnull Time timeout,
		File parentPath) {
		super(localAddressFuture, leaderRetriever, timeout, Collections.emptyMap());
		this.parentPath = parentPath;
	}

	@Override
	protected void respondAsLeader(ChannelHandlerContext ctx, RoutedRequest routed, RestfulGateway gateway) throws Exception {
		String[] relatedFiles = parentPath.list((dir, name) -> !(name.endsWith(".out") || name.endsWith(".err")));
		String body = JsonUtils.createSortedFilesJson(relatedFiles);
		byte[] bytes = body.getBytes(ENCODING);

		DefaultFullHttpResponse response = new DefaultFullHttpResponse(
			HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));

		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ENCODING.name());
		response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
		KeepAliveWrite.flush(ctx, routed.getRequest(), response);
	}
}
