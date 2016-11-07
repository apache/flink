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

package org.apache.flink.runtime.net.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.MediaType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.router.Routed;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;

/**
 * REST protocol support handler, providing server-side message encoding and decoding.
 */
@ChannelHandler.Sharable
public class RestServerProtocolHandler<T> extends ChannelHandlerAdapter {

	private final ObjectMapper mapper;
	private final Class<T> decoderValueType;

	public RestServerProtocolHandler(ObjectMapper mapper, Class<T> decoderValueType) {
		this.mapper = mapper;
		this.decoderValueType = decoderValueType;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) {
		ChannelPipeline cp = ctx.pipeline();

		if (cp.get(RestDecoder.class) == null) {
			ctx.pipeline().addBefore(ctx.name(), RestDecoder.class.getName(),
				new RestDecoder<T>(mapper, decoderValueType));
		}
		if (cp.get(RestEncoder.class) == null) {
			ctx.pipeline().addBefore(ctx.name(), RestEncoder.class.getName(),
				new RestEncoder(mapper));
		}
		cp.remove(ctx.name());
	}

	/**
	 * Encodes {@link RestResponse} messages as {@link FullHttpResponse} messages,
	 * using an {@link ObjectMapper} to serialize the content.
	 */
	@ChannelHandler.Sharable
	static class RestEncoder extends MessageToMessageEncoder<RestResponse> {

		private final ObjectMapper mapper;

		public RestEncoder(ObjectMapper mapper) {
			this.mapper = mapper;
		}

		@Override
		protected void encode(ChannelHandlerContext ctx, RestResponse msg, List<Object> out) throws Exception {

			ByteBuf contentBuf;
			boolean isError = RestUtils.isErrorResponse(msg);
			if((!isError && msg.content() != null) || (isError && msg.error() != null)) {
				contentBuf = ctx.alloc().buffer();
				try {
					try (ByteBufOutputStream stream = new ByteBufOutputStream(contentBuf)) {
						Object value = msg.content() != null ? msg.content() : msg.error();
						mapper.writeValue(stream, value);
					}
				}
				catch(Exception ex) {
					throw new IOException("Unable to encode the REST response content body.", ex);
				}
			}
			else {
				contentBuf = Unpooled.buffer(0);
			}

			DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
				msg.getProtocolVersion(), msg.getStatus(), contentBuf);
			httpResponse.headers().add(msg.headers());
			httpResponse.headers().set(HttpHeaders.Names.CONTENT_LENGTH, contentBuf.readableBytes());
			if(contentBuf.readableBytes() > 0) {
				httpResponse.headers().set(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
			}

			out.add(httpResponse);
		}
	}

	/**
	 * Decodes {@link Routed} messages as {@link RestRequest} messages,
	 * using an {@link ObjectMapper} to deserialize the content.
	 */
	@ChannelHandler.Sharable
	static class RestDecoder<T> extends MessageToMessageDecoder<Routed> {

		private final ObjectMapper mapper;
		private final Class<T> valueType;

		public RestDecoder(ObjectMapper mapper, Class<T> valueType) {
			this.mapper = mapper;
			this.valueType = valueType;
		}

		@Override
		public boolean acceptInboundMessage(Object msg) throws Exception {
			if(!super.acceptInboundMessage(msg)) {
				return false;
			}
			Routed routed = (Routed) msg;
			if(!(routed.request() instanceof FullHttpRequest)) {
				return false;
			}

			FullHttpRequest request = (FullHttpRequest) routed.request();

			if(request.content().readableBytes() > 0) {
				if (!request.headers().contains(HttpHeaders.Names.CONTENT_TYPE)) {
					return false;
				}
				MediaType contentType = MediaType.parse(request.headers().get(HttpHeaders.Names.CONTENT_TYPE));
				if (!contentType.is(MediaType.JSON_UTF_8.withoutParameters())) {
					return false;
				}
			}

			return true;
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, Routed routed, List<Object> out) throws Exception {

			FullHttpRequest request = (FullHttpRequest) routed.request();

			T content;
			if(request.content().readableBytes() > 0) {
				MediaType contentType = MediaType.parse(request.headers().get(HttpHeaders.Names.CONTENT_TYPE));
				Charset charset = contentType.charset().or(CharsetUtil.UTF_8);
				try {
					try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), charset)) {
						content = mapper.readValue(reader, valueType);
					}
				}
				catch(Exception ex) {
					throw new IOException("Unable to decode the REST request content body.", ex);
				}
			}
			else {
				content = null;
			}

			DefaultRestRequest restRequest = new DefaultRestRequest<>(
				request.getProtocolVersion(), request.getMethod(), request.getUri(), content);
			restRequest.headers().add(request.headers());

			out.add(restRequest);
		}
	}
}
