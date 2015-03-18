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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionRequestClientHandlerTest {

	/**
	 * Tests a fix for FLINK-1627.
	 *
	 * <p> FLINK-1627 discovered a race condition, which could lead to an infinite loop when a
	 * receiver was cancelled during a certain time of decoding a message. The test reproduces the
	 * input, which lead to the infinite loop: when the handler gets a reference to the buffer
	 * provider of the receiving input channel, but the respective input channel is released, the
	 * handler did not notice this from the buffer provider.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-1627">FLINK-1627</a>
	 */
	@Test(timeout=60000)
	public void testReleaseInputChannelDuringDecode() throws Exception {

		final RemoteInputChannel mockInputChannel = createMockReleasedInputChannel(
				new InputChannelID());

		final BufferResponse mockReceivedBuffer = createMockReceivedBuffer(
				mockInputChannel.getInputChannelId());

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(mockInputChannel);

		client.channelRead(mock(ChannelHandlerContext.class), mockReceivedBuffer);
	}

	/**
	 * Returns a mocked deserialized buffer message as it would be received during runtime.
	 */
	private BufferResponse createMockReceivedBuffer(InputChannelID channelId) throws IOException {

		// Mock buffer to serialize
		Buffer buffer = new Buffer(new MemorySegment(new byte[1024]), mock(BufferRecycler.class));
		BufferResponse resp = new BufferResponse(buffer, 0, channelId);

		ByteBuf serialized = resp.write(UnpooledByteBufAllocator.DEFAULT);

		// Skip general header bytes
		serialized.readBytes(NettyMessage.HEADER_LENGTH);

		BufferResponse deserialized = new BufferResponse();

		// Deserialize the bytes again. We have to go this way, because we only partly deserialize
		// the header of the response and wait for a buffer from the buffer pool to copy the payload
		// data into.
		deserialized.readFrom(serialized);

		return deserialized;
	}

	/**
	 * Returns a mocked input channel in a state as it was released during a decode.
	 */
	@SuppressWarnings("unchecked")
	private RemoteInputChannel createMockReleasedInputChannel(InputChannelID channelId) throws IOException {
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(null);
		when(bufferProvider.isDestroyed()).thenReturn(true);
		when(bufferProvider.addListener(any(EventListener.class))).thenReturn(false);

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(channelId);
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		return inputChannel;
	}
}
