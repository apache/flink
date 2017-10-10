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

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.DecoderException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.EncoderException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link JsonWebSocketMessageCodec}.
 */
public class JsonWebSocketMessageCodecTest {

	@Test
	public void testDecodeSuccess() {
		EmbeddedChannel channel = new EmbeddedChannel(new JsonWebSocketMessageCodec<>(TestMessage.class, TestMessage.class));
		channel.writeInbound(new TextWebSocketFrame("{\"sequenceNumber\":42}"));
		TestMessage actual = (TestMessage) channel.readInbound();
		Assert.assertNotNull(actual);
		Assert.assertEquals(42, actual.sequenceNumber);
	}

	@Test(expected = DecoderException.class)
	public void testDecodeFailure() {
		EmbeddedChannel channel = new EmbeddedChannel(new JsonWebSocketMessageCodec<>(TestMessage.class, TestMessage.class));
		channel.writeInbound(new TextWebSocketFrame(""));
	}

	@Test
	public void testEncodeSuccess() {
		EmbeddedChannel channel = new EmbeddedChannel(new JsonWebSocketMessageCodec<>(TestMessage.class, TestMessage.class));
		channel.writeOutbound(new TestMessage(42));
		TextWebSocketFrame actual = (TextWebSocketFrame) channel.readOutbound();
		Assert.assertNotNull(actual);
		Assert.assertEquals("{\"sequenceNumber\":42}", actual.text());
	}

	@Test(expected = EncoderException.class)
	public void testEncodeFailure() {
		EmbeddedChannel channel = new EmbeddedChannel(new JsonWebSocketMessageCodec<>(TestMessage.class, TestMessage.class));
		channel.writeOutbound(new TestMessage(-1));
	}

	private static class TestMessage {

		private final int sequenceNumber;

		public int getSequenceNumber() {
			if (sequenceNumber < 0) {
				throw new IllegalStateException();
			}
			return sequenceNumber;
		}

		@JsonCreator
		public TestMessage(@JsonProperty("sequenceNumber") int sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}
	}
}
