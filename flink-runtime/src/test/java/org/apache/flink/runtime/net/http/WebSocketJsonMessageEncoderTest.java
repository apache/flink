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

import com.google.common.base.Charsets;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link WebSocketJsonMessageEncoder}.
 */
public class WebSocketJsonMessageEncoderTest {

	@Test
	public void testEncodeSuccess() throws Exception {

		EmbeddedChannel channel = new EmbeddedChannel(
			new WebSocketJsonMessageEncoder<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expected = new TestUtils.TestEntity(1);
		channel.writeOutbound(expected);

		TextWebSocketFrame encoded = (TextWebSocketFrame) channel.outboundMessages().remove();
		TestUtils.TestEntity actual = TestUtils.deserialize(encoded.content(), Charsets.UTF_8, TestUtils.TestEntity.class);
		assertEquals(expected, actual);
	}

	@Test(expected = EncoderException.class)
	public void testEncodeFailure() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(
			new WebSocketJsonMessageEncoder<>(TestUtils.mapper, TestUtils.TestEntity.class));

		TestUtils.TestEntity expected = new TestUtils.UnwritableTestEntity();
		channel.writeOutbound(expected);
		channel.checkException();
	}

}
