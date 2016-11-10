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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.net.URI;

/**
 * Default WebSocket channel initializer.
 */
public class BaseWebSocketClientChannelInitializer extends ChannelInitializer<Channel> {

	private final URI webSocketURL;
	private final int maxFramePayloadLength;

	/**
	 * @param webSocketURL the URL for the HTTP resource to handshake with.
	 * @param maxFramePayloadLength the max length.
	*/
	public BaseWebSocketClientChannelInitializer(URI webSocketURL, int maxFramePayloadLength) {
		this.webSocketURL = webSocketURL;
		this.maxFramePayloadLength = maxFramePayloadLength;
	}

	@Override
	protected void initChannel(Channel ch) throws Exception {
		ch.pipeline().addLast(new HttpClientCodec());
		ch.pipeline().addLast(new HttpObjectAggregator(maxFramePayloadLength));
		ch.pipeline().addLast(new WebSocketClientProtocolHandler(
			webSocketURL, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, maxFramePayloadLength));
		ch.pipeline().addLast(new WebSocketFrameAggregator(maxFramePayloadLength));
	}
}
