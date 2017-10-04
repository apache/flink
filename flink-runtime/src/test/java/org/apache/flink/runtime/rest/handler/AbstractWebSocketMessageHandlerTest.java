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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.apache.flink.shaded.netty4.io.netty.util.AbstractReferenceCounted;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link AbstractWebSocketMessageHandler}.
 */
public class AbstractWebSocketMessageHandlerTest {

	/**
	 * Validates that the handler correctly processes handshake completion.
	 */
	@Test
	public void testHandshakeComplete() throws Exception {
		TestWebSocketMessageHandler handler = new TestWebSocketMessageHandler();
		EmbeddedChannel channel = new EmbeddedChannel(new Driver(), handler);
		Assert.assertTrue(handler.handshakeCompleted);
	}

	/**
	 * Tests message writing.
	 */
	@Test
	public void testWriteMessage() throws Exception {
		TestWebSocketMessageHandler handler = new TestWebSocketMessageHandler();
		EmbeddedChannel channel = new EmbeddedChannel(new Driver(), handler);

		TestMessage expected = new TestMessage();
		channel.writeAndFlush(expected);
		Assert.assertEquals(0, expected.refCnt());
		TextWebSocketFrame actual = (TextWebSocketFrame) channel.readOutbound();
		Assert.assertNotNull(actual);
		Assert.assertEquals(1, actual.refCnt());
	}

	/**
	 * Tests passthrough of HTTP responses as written by the websocket handshake handler.
	 */
	@Test
	public void testWritePassthrough() throws Exception {
		TestWebSocketMessageHandler handler = new TestWebSocketMessageHandler();
		EmbeddedChannel channel = new EmbeddedChannel(new Driver(), handler);

		DefaultFullHttpResponse expected = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SWITCHING_PROTOCOLS);
		channel.writeAndFlush(expected);
		DefaultFullHttpResponse actual = (DefaultFullHttpResponse) channel.readOutbound();
		Assert.assertSame(expected, actual);
		Assert.assertEquals(1, actual.refCnt());
	}

	/**
	 * Tests exception handling.
	 */
	@Test
	public void testExceptionHandling() throws Exception {
		TestWebSocketMessageHandler handler = new TestWebSocketMessageHandler();
		EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
			@Override
			public void channelActive(ChannelHandlerContext ctx) throws Exception {
				super.channelActive(ctx);
				// fire an exception as would the websocket protocol handler
				ctx.fireExceptionCaught(new Exception());
			}
		}, handler);

		Assert.assertTrue(!channel.isOpen());
	}

	private static class Driver extends ChannelInboundHandlerAdapter {
		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			super.channelActive(ctx);
			ctx.fireUserEventTriggered(WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_COMPLETE);
		}
	}

	private static class TestWebSocketMessageHandler extends AbstractWebSocketMessageHandler {
		boolean handshakeCompleted = false;

		@Override
		protected void handshakeComplete(ChannelHandlerContext ctx) throws Exception {
			handshakeCompleted = true;
		}
	}

	private static class TestMessage extends AbstractReferenceCounted implements ResponseBody {
		@Override
		protected void deallocate() {
		}
	}
}
