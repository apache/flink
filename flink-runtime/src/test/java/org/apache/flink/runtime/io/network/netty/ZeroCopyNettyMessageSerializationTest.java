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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes with
 * the zero-copy netty handlers.
 */
public class ZeroCopyNettyMessageSerializationTest extends NettyMessageSerializationTestBase {
	private final EmbeddedChannel channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // outbound messages
			new ZeroCopyNettyMessageDecoder(new CreditedBasedReceiverSideNetworkBufferAllocator(createPartitionRequestClientHandler())));

	@Override
	public EmbeddedChannel getChannel() {
		return channel;
	}

	private CreditBasedPartitionRequestClientHandler createPartitionRequestClientHandler() {
		CreditBasedPartitionRequestClientHandler handler = mock(CreditBasedPartitionRequestClientHandler.class);

		RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.requestBuffer()).thenAnswer(new Answer<Buffer>() {
			@Override
			public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(1024), FreeingBufferRecycler.INSTANCE);
			}
		});
		when(handler.getInputChannel(any(InputChannelID.class))).thenReturn(inputChannel);

		return handler;
	}
}
