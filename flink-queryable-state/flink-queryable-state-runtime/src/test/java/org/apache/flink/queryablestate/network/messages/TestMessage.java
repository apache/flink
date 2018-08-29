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

package org.apache.flink.queryablestate.network.messages;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Message with a string as payload.
 */
public class TestMessage extends MessageBody {

	private final String message;

	public TestMessage(String message) {
		this.message = Preconditions.checkNotNull(message);
	}

	public String getMessage() {
		return message;
	}

	@Override
	public byte[] serialize() {
		byte[] content = message.getBytes(ConfigConstants.DEFAULT_CHARSET);

		// message size + 4 for the length itself
		return ByteBuffer.allocate(content.length + Integer.BYTES)
			.putInt(content.length)
			.put(content)
			.array();
	}

	/**
	 * The deserializer for our {@link TestMessage test messages}.
	 */
	public static class TestMessageDeserializer implements MessageDeserializer<TestMessage> {

		@Override
		public TestMessage deserializeMessage(ByteBuf buf) {
			int length = buf.readInt();
			String message = "";
			if (length > 0) {
				byte[] name = new byte[length];
				buf.readBytes(name);
				message = new String(name, ConfigConstants.DEFAULT_CHARSET);
			}
			return new TestMessage(message);
		}
	}
}
