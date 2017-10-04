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

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.websocket.KeyedChannelRouter;
import org.apache.flink.runtime.rest.websocket.WebSocketHandlerUtils;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.ServerHandshakeStateEvent;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super class for netty-based websocket message handlers.
 *
 * <p>A message handler subclass is registered into the channel pipeline by
 * the {@link AbstractRestHandler} following a websocket handshake request.
 * Once registered, the message handler typically reacts to handshake success by:
 *  - initiating an application-level interaction with the client
 *  - registering the channel with a {@link KeyedChannelRouter}
 */
public abstract class AbstractWebSocketMessageHandler extends ChannelDuplexHandler {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof ServerHandshakeStateEvent) {
			ServerHandshakeStateEvent handshakeEvent = (ServerHandshakeStateEvent) evt;
			if (handshakeEvent == ServerHandshakeStateEvent.HANDSHAKE_COMPLETE) {
				log.debug("Handshake completed for channel {}.", ctx.channel().remoteAddress());
				handshakeComplete(ctx);
			}
		}

		super.userEventTriggered(ctx, evt);
	}

	protected abstract void handshakeComplete(ChannelHandlerContext ctx) throws Exception;

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof ResponseBody) {
			// write the message as a websocket text frame
			WebSocketHandlerUtils.sendMessage(ctx, (ResponseBody) msg);
			ReferenceCountUtil.release(msg);
		}
		else {
			super.write(ctx, msg, promise);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		log.error("WebSocket channel error; closing the channel.", cause);
		ctx.close();
	}
}
