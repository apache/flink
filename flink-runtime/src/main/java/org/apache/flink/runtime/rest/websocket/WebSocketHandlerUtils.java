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

package org.apache.flink.runtime.rest.websocket;

import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Utilities for WebSocket handlers.
 */
public class WebSocketHandlerUtils {
	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	/**
	 * Sends the websocket response to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param response which should be sent
	 * @param <P> type of the response
	 */
	public static <P extends ResponseBody> void sendMessage(
		ChannelHandlerContext channelHandlerContext,
		P response) {

		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, response);
		} catch (IOException ioe) {
			sendErrorMessage(channelHandlerContext, new ErrorResponseBody("Internal server error. Could not map response to JSON."));
			return;
		}
		sendMessageInternal(channelHandlerContext, sw.toString()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
	}

	/**
	 * Sends the websocket error message to the given channel.
	 *
	 * @param channelHandlerContext identifying the open channel
	 * @param errorMessage which should be sent
	 */
	public static void sendErrorMessage(
		ChannelHandlerContext channelHandlerContext,
		ErrorResponseBody errorMessage) {

		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, errorMessage);
		} catch (IOException e) {
			// this should never happen
			throw new FlinkRuntimeException("Could not map error response to JSON.", e);
		}
		sendMessageInternal(channelHandlerContext, sw.toString()).addListener(ChannelFutureListener.CLOSE);
	}

	private static <P extends ResponseBody> ChannelFuture sendMessageInternal(
		ChannelHandlerContext channelHandlerContext,
		String text) {

		TextWebSocketFrame frame = new TextWebSocketFrame(text);
		return channelHandlerContext.writeAndFlush(frame);
	}

	public static TextWebSocketFrame encodeMessage(ResponseBody message) throws IOException {
		StringWriter sw = new StringWriter();
		mapper.writeValue(sw, message);
		return new TextWebSocketFrame(sw.toString());
	}

	public static <T extends ResponseBody> T decodeMessage(TextWebSocketFrame frame, Class<T> clazz) throws IOException {
		return mapper.readValue(frame.text(), clazz);
	}
}
