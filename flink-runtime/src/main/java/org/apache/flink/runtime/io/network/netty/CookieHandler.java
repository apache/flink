/**
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

package org.apache.flink.runtime.io.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CookieHandler {

	public static class ClientCookieHandler extends ChannelInboundHandlerAdapter {

		private final Logger LOG = LoggerFactory.getLogger(ClientCookieHandler.class);

		private final String secureCookie;

		final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

		public ClientCookieHandler(String secureCookie) {
			this.secureCookie = secureCookie;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			super.channelActive(ctx);
			LOG.debug("In channelActive method of ClientCookieHandler");

			if(this.secureCookie != null && this.secureCookie.length() != 0) {
				LOG.debug("In channelActive method of ClientCookieHandler -> sending secure cookie");
				final ByteBuf buffer = Unpooled.buffer(4 + this.secureCookie.getBytes(DEFAULT_CHARSET).length);
				buffer.writeInt(secureCookie.getBytes(DEFAULT_CHARSET).length);
				buffer.writeBytes(secureCookie.getBytes(DEFAULT_CHARSET));
				ctx.writeAndFlush(buffer);
			}
		}
	}

	public static class ServerCookieDecoder extends MessageToMessageDecoder<ByteBuf> {

		private final String secureCookie;

		private final List<Channel> channelList = new ArrayList<>();

		private final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

		private final Logger LOG = LoggerFactory.getLogger(ServerCookieDecoder.class);

		public ServerCookieDecoder(String secureCookie) {
			this.secureCookie = secureCookie;
		}

		/**
		 * Decode from one message to an other. This method will be called for each written message that can be handled
		 * by this encoder.
		 *
		 * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder} belongs to
		 * @param msg the message to decode to an other one
		 * @param out the {@link List} to which decoded messages should be added
		 * @throws Exception is thrown if an error accour
		 */
		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

			LOG.debug("ChannelHandlerContext name: {}, channel: {}", ctx.name(), ctx.channel());

			if(secureCookie == null || secureCookie.length() == 0) {
				LOG.debug("Not validating secure cookie since the server configuration is not enabled to use cookie");
				return;
			}

			LOG.debug("Going to decode the secure cookie passed by the remote client");

			if(channelList.contains(ctx.channel())) {
				LOG.debug("Channel: {} already authorized", ctx.channel());
				return;
			}

			//read cookie based on the cookie length passed
			int cookieLength = msg.readInt();
			if(cookieLength != secureCookie.getBytes(DEFAULT_CHARSET).length) {
				ctx.channel().close();
				String message = "Cookie length does not match with source cookie. Invalid secure cookie passed.";
				throw new IllegalStateException(message);
			}

			//read only if cookie length is greater than zero
			if(cookieLength > 0) {

				final byte[] buffer = new byte[secureCookie.getBytes(DEFAULT_CHARSET).length];
				msg.readBytes(buffer, 0, cookieLength);

				if(!Arrays.equals(secureCookie.getBytes(DEFAULT_CHARSET), buffer)) {
					LOG.error("Secure cookie from the client is not matching with the server's identity");
					throw new IllegalStateException("Invalid secure cookie passed.");
				}

				LOG.info("Secure cookie validation passed");

				channelList.add(ctx.channel());
			}

		}
	}
}