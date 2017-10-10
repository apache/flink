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

import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.CombinedChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.DecoderException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.EncoderException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

/**
 * A codec for JSON-encoded WebSocket messages.
 *
 * @param <I> the inbound message type (converted from JSON).
 * @param <O> the outbound message type (converted to JSON).
 */
@ChannelHandler.Sharable
public class JsonWebSocketMessageCodec<I, O> extends CombinedChannelDuplexHandler<JsonWebSocketMessageCodec.WebSocketMessageDecoder<I>, JsonWebSocketMessageCodec.WebSocketMessageEncoder<O>> {

	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	public JsonWebSocketMessageCodec(Class<I> inboundMessageClass, Class<O> outboundMessageClass) {
		super(new WebSocketMessageDecoder<>(inboundMessageClass), new WebSocketMessageEncoder<>(outboundMessageClass));
	}

	@ChannelHandler.Sharable
	static class WebSocketMessageDecoder<I> extends MessageToMessageDecoder<TextWebSocketFrame> {

		private final ObjectReader reader;

		public WebSocketMessageDecoder(Class<I> messageClass) {
			reader = mapper.readerFor(messageClass);
		}

		@Override
		protected void decode(ChannelHandlerContext channelHandlerContext, TextWebSocketFrame frame, List<Object> list) throws Exception {
			try {
				try (StringReader sr = new StringReader(frame.text())) {
					I i = reader.readValue(sr);
					list.add(i);
				}
			}
			catch (Exception e) {
				throw new DecoderException("Unable to decode the WebSocket frame as a JSON object", e);
			}
		}
	}

	@ChannelHandler.Sharable
	static class WebSocketMessageEncoder<O> extends MessageToMessageEncoder<O> {

		private final ObjectWriter writer;

		protected WebSocketMessageEncoder(Class<O> messageClass) {
			super(messageClass);
			writer = mapper.writerFor(messageClass);
		}

		@Override
		protected void encode(ChannelHandlerContext ctx, O o, List<Object> list) throws Exception {
			try {
				StringWriter sw = new StringWriter();
				writer.writeValue(sw, o);
				list.add(new TextWebSocketFrame(sw.toString()));
			}
			catch (Exception e) {
				throw new EncoderException("Unable to encode the JSON object as a WebSocket frame", e);
			}
		}
	}
}
