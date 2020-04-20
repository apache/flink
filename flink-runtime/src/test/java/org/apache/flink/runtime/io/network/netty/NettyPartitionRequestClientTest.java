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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.mockConnectionManagerWithPartitionRequestClient;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link NettyPartitionRequestClient}.
 */
public class NettyPartitionRequestClientTest {

	@Test
	public void testRetriggerPartitionRequest() throws Exception {
		final long deadline = System.currentTimeMillis() + 30_000L; // 30 secs

		final CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		final EmbeddedChannel channel = new EmbeddedChannel(handler);
		final PartitionRequestClient client = new NettyPartitionRequestClient(
			channel, handler, mock(ConnectionID.class), mock(PartitionRequestClientFactory.class));

		final int numExclusiveBuffers = 2;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32, numExclusiveBuffers);
		final SingleInputGate inputGate = createSingleInputGate(1);
		final RemoteInputChannel inputChannel = InputChannelBuilder.newBuilder()
			.setConnectionManager(mockConnectionManagerWithPartitionRequestClient(client))
			.setInitialBackoff(1)
			.setMaxBackoff(2)
			.setMemorySegmentProvider(networkBufferPool)
			.buildRemoteChannel(inputGate);

		try {
			inputGate.setInputChannels(inputChannel);
			final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();

			// first subpartition request
			inputChannel.requestSubpartition(0);

			assertTrue(channel.isWritable());
			Object readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
			assertEquals(inputChannel.getInputChannelId(), ((PartitionRequest) readFromOutbound).receiverId);
			assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

			// retrigger subpartition request, e.g. due to failures
			inputGate.retriggerPartitionRequest(inputChannel.getPartitionId().getPartitionId());
			runAllScheduledPendingTasks(channel, deadline);

			readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
			assertEquals(inputChannel.getInputChannelId(), ((PartitionRequest) readFromOutbound).receiverId);
			assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

			// retrigger subpartition request once again, e.g. due to failures
			inputGate.retriggerPartitionRequest(inputChannel.getPartitionId().getPartitionId());
			runAllScheduledPendingTasks(channel, deadline);

			readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
			assertEquals(inputChannel.getInputChannelId(), ((PartitionRequest) readFromOutbound).receiverId);
			assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

			assertNull(channel.readOutbound());
		} finally {
			// Release all the buffer resources
			inputGate.close();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	@Test
	public void testDoublePartitionRequest() throws Exception {
		final CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		final EmbeddedChannel channel = new EmbeddedChannel(handler);
		final PartitionRequestClient client = new NettyPartitionRequestClient(
			channel, handler, mock(ConnectionID.class), mock(PartitionRequestClientFactory.class));

		final int numExclusiveBuffers = 2;
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32, numExclusiveBuffers);
		final SingleInputGate inputGate = createSingleInputGate(1);
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, client, networkBufferPool);

		try {
			inputGate.setInputChannels(inputChannel);
			final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
			inputGate.setBufferPool(bufferPool);
			inputGate.assignExclusiveSegments();
			inputChannel.requestSubpartition(0);

			// The input channel should only send one partition request
			assertTrue(channel.isWritable());
			Object readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(PartitionRequest.class));
			assertEquals(inputChannel.getInputChannelId(), ((PartitionRequest) readFromOutbound).receiverId);
			assertEquals(numExclusiveBuffers, ((PartitionRequest) readFromOutbound).credit);

			assertNull(channel.readOutbound());
		} finally {
			// Release all the buffer resources
			inputGate.close();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	/**
	 * Run all pending scheduled tasks (waits until all tasks have been run or the deadline has passed.
	 *
	 * @param channel  the channel to execute tasks for
	 * @param deadline maximum timestamp in ms to stop waiting further
	 * @throws InterruptedException
	 */
	void runAllScheduledPendingTasks(EmbeddedChannel channel, long deadline) throws InterruptedException {
		// NOTE: we don't have to be super fancy here; busy-polling with 1ms delays is enough
		while (channel.runScheduledPendingTasks() != -1 && System.currentTimeMillis() < deadline) {
			Thread.sleep(1);
		}
	}
}
