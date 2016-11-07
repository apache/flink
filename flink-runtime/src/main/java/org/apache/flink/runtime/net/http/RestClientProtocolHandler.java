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
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * REST protocol support handler, providing client-side message encoding and decoding.
 */
@ChannelHandler.Sharable
public class RestClientProtocolHandler<T> extends ChannelHandlerAdapter {

	private final ObjectMapper mapper;
	private final Class<T> decoderValueType;

	public RestClientProtocolHandler(ObjectMapper mapper, Class<T> decoderValueType) {
		this.mapper = checkNotNull(mapper);
		this.decoderValueType = checkNotNull(decoderValueType);
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
	}

	/**
	 * Encodes {@link RestRequest} messages as {@link FullHttpRequest} messages,
	 * using an {@link ObjectMapper} to serialize the content.
	 */
	@ChannelHandler.Sharable
	static class RestEncoder extends MessageToMessageEncoder<RestRequest> {

		private final ObjectMapper mapper;

		public RestEncoder(ObjectMapper mapper) {
			this.mapper = mapper;
		}

		@Override
		protected void encode(ChannelHandlerContext ctx, RestRequest msg, List<Object> out) throws Exception {

			ByteBuf contentBuf;
			if(msg.content() != null) {
				contentBuf = ctx.alloc().buffer();
				try {
					try (ByteBufOutputStream stream = new ByteBufOutputStream(contentBuf)) {
						mapper.writeValue(stream, msg.content());
					}
				}
				catch(Exception ex) {
					throw new IOException("Unable to encode the REST request content body.", ex);
				}
			}
			else {
				contentBuf = Unpooled.buffer(0);
			}

			FullHttpRequest httpRequest = new DefaultFullHttpRequest(
				msg.getProtocolVersion(), msg.getMethod(), msg.getUri(), contentBuf, false);
			httpRequest.headers().add(HttpHeaders.Names.CONTENT_LENGTH, contentBuf.readableBytes());
			httpRequest.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());

			out.add(httpRequest);
		}
	}

	/**
	 * Decodes {@link FullHttpResponse} messages as {@link RestResponse} messages,
	 * using an {@link ObjectMapper} to deserialize the content.
	 */
	@ChannelHandler.Sharable
	static class RestDecoder<T> extends MessageToMessageDecoder<FullHttpResponse> {

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
			FullHttpResponse response = (FullHttpResponse) msg;
			if(response.content().readableBytes() > 0) {
				if (!response.headers().contains(HttpHeaders.Names.CONTENT_TYPE)) {
					return false;
				}
				MediaType contentType = MediaType.parse(response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
				if(!contentType.is(MediaType.JSON_UTF_8.withoutParameters())) {
					return false;
				}
			}

			return true;
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, FullHttpResponse response, List<Object> out) throws Exception {

			T content;
			RestError error;
			if(response.content().readableBytes() > 0) {
				MediaType contentType = MediaType.parse(response.headers().get(HttpHeaders.Names.CONTENT_TYPE));
				Charset charset = contentType.charset().or(CharsetUtil.UTF_8);
				try {
					try (Reader reader = new InputStreamReader(new ByteBufInputStream(response.content()), charset)) {

						if(RestUtils.isErrorResponse(response)) {
							content = null;
							error = mapper.readValue(reader, RestError.class);
						}
						else {
							content = mapper.readValue(reader, valueType);
							error = null;
						}
					}
				}
				catch(Exception ex) {
					throw new IOException("Unable to decode the REST response content body.", ex);
				}
			}
			else {
				content = null;
				error = null;
			}

			DefaultRestResponse restResponse = new DefaultRestResponse<T>(
				response.getProtocolVersion(), response.getStatus(), content, true, error);
			restResponse.headers().add(response.headers());

			out.add(restResponse);
		}
	}
}
