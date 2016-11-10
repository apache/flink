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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.router.Routed;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Upgrades a channel to a WebSocket connection in response to a routed HTTP request.
 *
 * For example, an HTTP request of "POST /subscribe" could be routed to this handler
 * to upgrade the connection to a WebSocket.   The underlying (Netty-provided)
 * {@link WebSocketServerProtocolHandler} blindly upgrades all channels; this class allows
 * the upgrade to be selective.
 */
@ChannelHandler.Sharable
public class RoutedWebSocketHandshakeHandler extends SimpleChannelInboundHandler<Routed> {

	private static final Logger LOG = LoggerFactory.getLogger(RoutedWebSocketHandshakeHandler.class);

	private final ChannelHandler postHandshakeHandler;

	private final ChannelGroup channelGroup;

	/**
	 * Construct a handshake handler that upgrades the request then installs the given
	 * post-handshake handler.
	 * @param postHandshakeHandler the post-handshake handler to install.
	 * @param channelGroup (optional) a channel group to add the upgraded channels to.
     */
	public RoutedWebSocketHandshakeHandler(ChannelHandler postHandshakeHandler, ChannelGroup channelGroup) {
		this.postHandshakeHandler = checkNotNull(postHandshakeHandler);
		this.channelGroup = channelGroup;
	}

	/**
	 * Get the channel group containing upgraded channels.
     */
	public ChannelGroup getChannelGroup() {
		return channelGroup;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		LOG.debug("Received handshake request at {} for channel {}",
			routed.path(), ctx.channel().remoteAddress());

		// inject the websocket protocol handler into this channel, to be active
		// until the channel is closed.  note that the handshake may/or complete synchronously.
		ctx.pipeline().addAfter(ctx.name(), WebSocketServerProtocolHandler.class.getName(),
			new WebSocketServerProtocolHandler(routed.path()));

		// inject a handler for handshake success
		ctx.pipeline().addAfter(WebSocketServerProtocolHandler.class.getName(), HandshakeCompleteHandler.class.getName(),
			new HandshakeCompleteHandler());

		// forward the message to the installed handler
		HttpRequest request = routed.request();
		ReferenceCountUtil.retain(request);
		ctx.fireChannelRead(request);
	}

	/**
	 * Handles the HANDSHAKE_COMPLETE event by installing the provided post-handshake handler.
	 */
	class HandshakeCompleteHandler extends ChannelInboundHandlerAdapter {

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof WebSocketServerProtocolHandler.ServerHandshakeStateEvent) {
				WebSocketServerProtocolHandler.ServerHandshakeStateEvent handshakeEvent =
					(WebSocketServerProtocolHandler.ServerHandshakeStateEvent) evt;
				if (handshakeEvent == WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE) {
					LOG.debug("Handshake completed for channel {}.", ctx.channel().remoteAddress());

					if(channelGroup != null) {
						// add the channel to the provided channel group, for bulk messaging purposes
						channelGroup.add(ctx.channel());
					}

					// install the post-handshake handler
					ctx.pipeline().replace(ctx.name(), ctx.name() + "-after", postHandshakeHandler);

					return;
				}
			}

			super.userEventTriggered(ctx, evt);
		}
	}
}
