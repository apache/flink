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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes
 * with the non-zero-copy netty handlers.
 */
public class NettyMessageSerializationTest extends NettyMessageSerializationTestBase {

	public static final boolean RESTORE_OLD_NETTY_BEHAVIOUR = false;

	private final EmbeddedChannel channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // outbound messages
			new NettyMessage.NettyMessageDecoder(RESTORE_OLD_NETTY_BEHAVIOUR)); // inbound messages

	@Override
	public EmbeddedChannel getChannel() {
		return channel;
	}

	@Override
	public boolean bufferIsReleasedOnDecoding() {
		// Netty 4.1 is not copying the messages, but retaining slices of them. BufferResponse actual is in this case
		// holding a reference to the buffer. Buffer will be recycled only once "actual" will be released.
		return false;
	}
}
