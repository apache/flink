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

import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.router.Handler;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

/**
 * Tests the RoutedWebSocketHandshakeHandler.
 */
public class RoutedWebSocketHandshakeHandlerTest {

	class TestClientHandler extends BaseWebSocketClientChannelInitializer {
		public TestClientHandler(URI uri) {
			super(uri, Short.MAX_VALUE);
		}
	}

	class TestServerHandler extends ChannelHandlerAdapter {
	}

	@Test()
	public void testHandshake() throws Exception {

		ChannelGroup testGroup = new DefaultChannelGroup(ImmediateEventExecutor.INSTANCE);

		TestServerHandler handler = new TestServerHandler();
		EmbeddedChannel serverChannel = new EmbeddedChannel(
			new HttpServerCodec(),
			new HttpObjectAggregator(Short.MAX_VALUE),
			new Handler(TestUtils.router),
			new RoutedWebSocketHandshakeHandler(new TestServerHandler(), testGroup));

		EmbeddedChannel clientChannel = new EmbeddedChannel(new TestClientHandler(new URI("/")));

		assertNull(serverChannel.pipeline().get(TestServerHandler.class));

		// pump the handshake messages to complete the connection
		serverChannel.writeInbound(clientChannel.outboundMessages().remove());
		clientChannel.writeInbound(serverChannel.outboundMessages().remove());

		// verify
		assertTrue(serverChannel.isActive());
		assertTrue(testGroup.contains(serverChannel));
		assertNotNull(serverChannel.pipeline().get(TestServerHandler.class));
	}
}
