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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;

/**
 * Same as {@link org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectDecoder}
 * but returns HTTP 413 to the client if the payload exceeds {@link #maxContentLength}.
 */
public class FlinkHttpObjectAggregator extends org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator {

	private final Map<String, String> responseHeaders;

	public FlinkHttpObjectAggregator(final int maxContentLength, @Nonnull final Map<String, String> responseHeaders) {
		super(maxContentLength);
		this.responseHeaders = responseHeaders;
	}

	@Override
	protected void decode(
			final ChannelHandlerContext ctx,
			final HttpObject msg,
			final List<Object> out) throws Exception {

		try {
			super.decode(ctx, msg, out);
		} catch (final TooLongFrameException e) {
			HandlerUtils.sendErrorResponse(
				ctx,
				false,
				new ErrorResponseBody(String.format(
					e.getMessage() + " Try to raise [%s]",
					RestOptions.SERVER_MAX_CONTENT_LENGTH.key())),
				HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
				responseHeaders);
		}
	}
}
