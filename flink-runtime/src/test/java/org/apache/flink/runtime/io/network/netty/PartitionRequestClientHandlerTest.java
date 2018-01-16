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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionRequestClientHandlerTest {

	/**
	 * Tests a fix for FLINK-1627.
	 *
	 * <p> FLINK-1627 discovered a race condition, which could lead to an infinite loop when a
	 * receiver was cancelled during a certain time of decoding a message. The test reproduces the
	 * input, which lead to the infinite loop: when the handler gets a reference to the buffer
	 * provider of the receiving input channel, but the respective input channel is released (and
	 * the corresponding buffer provider destroyed), the handler did not notice this.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-1627">FLINK-1627</a>
	 */
	@Test(timeout = 60000)
	@SuppressWarnings("unchecked")
	public void testReleaseInputChannelDuringDecode() throws Exception {
		// Mocks an input channel in a state as it was released during a decode.
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(null);
		when(bufferProvider.isDestroyed()).thenReturn(true);
		when(bufferProvider.addBufferListener(any(BufferListener.class))).thenReturn(false);

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		final BufferResponse receivedBuffer = createBufferResponse(
				TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0, inputChannel.getInputChannelId(), 2);

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(inputChannel);

		client.channelRead(mock(ChannelHandlerContext.class), receivedBuffer);
	}

	/**
	 * Tests a fix for FLINK-1761.
	 *
	 * <p>FLINK-1761 discovered an IndexOutOfBoundsException, when receiving buffers of size 0.
	 */
	@Test
	public void testReceiveEmptyBuffer() throws Exception {
		// Minimal mock of a remote input channel
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer(0));

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		// An empty buffer of size 0
		final Buffer emptyBuffer = TestBufferFactory.createBuffer(0);

		final int backlog = 2;
		final BufferResponse receivedBuffer = createBufferResponse(
			emptyBuffer, 0, inputChannel.getInputChannelId(), backlog);

		final CreditBasedClientHandler client = new CreditBasedClientHandler();
		client.addInputChannel(inputChannel);

		// Read the empty buffer
		client.channelRead(mock(ChannelHandlerContext.class), receivedBuffer);

		// This should not throw an exception
		verify(inputChannel, never()).onError(any(Throwable.class));
		verify(inputChannel, times(1)).onEmptyBuffer(0, backlog);
	}

	/**
	 * Verifies that {@link RemoteInputChannel#onBuffer(Buffer, int, int)} is called when a
	 * {@link BufferResponse} is received.
	 */
	@Test
	public void testReceiveBuffer() throws Exception {
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.getPartitionId().getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(8, 8);
			inputGate.setBufferPool(bufferPool);
			final int numExclusiveBuffers = 2;
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			final CreditBasedClientHandler handler = new CreditBasedClientHandler();
			handler.addInputChannel(inputChannel);

			final int backlog = 2;
			final BufferResponse bufferResponse = createBufferResponse(
				TestBufferFactory.createBuffer(32), 0, inputChannel.getInputChannelId(), backlog);
			handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);

			assertEquals(1, inputChannel.getNumberOfQueuedBuffers());
			assertEquals(2, inputChannel.getSenderBacklog());
		} finally {
			// Release all the buffer resources
			inputGate.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	/**
	 * Verifies that {@link RemoteInputChannel#onError(Throwable)} is called when a
	 * {@link BufferResponse} is received but no available buffer in input channel.
	 */
	@Test
	public void testThrowExceptionForNoAvailableBuffer() throws Exception {
		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel = spy(createRemoteInputChannel(inputGate));

		final CreditBasedClientHandler handler = new CreditBasedClientHandler();
		handler.addInputChannel(inputChannel);

		assertEquals("There should be no buffers available in the channel.",
			0, inputChannel.getNumberOfAvailableBuffers());

		final BufferResponse bufferResponse = createBufferResponse(
			TestBufferFactory.createBuffer(TestBufferFactory.BUFFER_SIZE), 0, inputChannel.getInputChannelId(), 2);
		handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);

		verify(inputChannel, times(1)).onError(any(IllegalStateException.class));
	}

	/**
	 * Verifies that {@link RemoteInputChannel#onFailedPartitionRequest()} is called when a
	 * {@link PartitionNotFoundException} is received.
	 */
	@Test
	public void testReceivePartitionNotFoundException() throws Exception {
		// Minimal mock of a remote input channel
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer(0));

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		final ErrorResponse partitionNotFound = new ErrorResponse(
			new PartitionNotFoundException(new ResultPartitionID()),
			inputChannel.getInputChannelId());

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(inputChannel);

		// Mock channel context
		ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		when(ctx.channel()).thenReturn(mock(Channel.class));

		client.channelActive(ctx);

		client.channelRead(ctx, partitionNotFound);

		verify(inputChannel, times(1)).onFailedPartitionRequest();
	}

	@Test
	public void testCancelBeforeActive() throws Exception {

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(inputChannel);

		// Don't throw NPE
		client.cancelRequestFor(null);

		// Don't throw NPE, because channel is not active yet
		client.cancelRequestFor(inputChannel.getInputChannelId());
	}

	/**
	 * Verifies that {@link RemoteInputChannel} is enqueued in the pipeline for notifying credits,
	 * and verifies the behaviour of credit notification by triggering channel's writability changed.
	 */
	@Test
	public void testNotifyCreditAvailable() throws Exception {
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel1 = createRemoteInputChannel(inputGate);
		final RemoteInputChannel inputChannel2 = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel1.getPartitionId().getPartitionId(), inputChannel1);
		inputGate.setInputChannel(inputChannel2.getPartitionId().getPartitionId(), inputChannel2);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
			inputGate.setBufferPool(bufferPool);
			final int numExclusiveBuffers = 2;
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			final CreditBasedClientHandler handler = new CreditBasedClientHandler();
			final EmbeddedChannel channel = new EmbeddedChannel(handler);

			// The PartitionRequestClient is tied to PartitionRequestClientHandler currently, so we
			// have to add input channels in CreditBasedClientHandler explicitly
			inputChannel1.requestSubpartition(0);
			inputChannel2.requestSubpartition(0);
			handler.addInputChannel(inputChannel1);
			handler.addInputChannel(inputChannel2);

			// The buffer response will take one available buffer from input channel, and it will trigger
			// requesting (backlog + numExclusiveBuffers -  numAvailableBuffers) floating buffers
			final BufferResponse bufferResponse1 = createBufferResponse(
				TestBufferFactory.createBuffer(32), 0, inputChannel1.getInputChannelId(), 1);
			final BufferResponse bufferResponse2 = createBufferResponse(
				TestBufferFactory.createBuffer(32), 0, inputChannel2.getInputChannelId(), 1);
			handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse1);
			handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse2);

			// The PartitionRequestClient is tied to PartitionRequestClientHandler currently, so we
			// have to notify credit available in CreditBasedClientHandler explicitly
			handler.notifyCreditAvailable(inputChannel1);
			handler.notifyCreditAvailable(inputChannel2);

			assertEquals(2, inputChannel1.getUnannouncedCredit());
			assertEquals(2, inputChannel2.getUnannouncedCredit());

			channel.runPendingTasks();

			// The two input channels should notify credits via writable channel
			assertTrue(channel.isWritable());
			Object readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(AddCredit.class));
			assertEquals(2, ((AddCredit) readFromOutbound).credit);
			readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(AddCredit.class));
			assertEquals(2, ((AddCredit) readFromOutbound).credit);
			assertNull(channel.readOutbound());

			final int highWaterMark = channel.config().getWriteBufferHighWaterMark();
			// Set the writer index to the high water mark to ensure that all bytes are written
			// to the wire although the buffer is "empty".
			ByteBuf channelBlockingBuffer = Unpooled.buffer(highWaterMark).writerIndex(highWaterMark);
			channel.write(channelBlockingBuffer);

			// Trigger notify credits available via buffer response on the condition of un-writable channel
			final BufferResponse bufferResponse3 = createBufferResponse(
				TestBufferFactory.createBuffer(32), 1, inputChannel1.getInputChannelId(), 1);
			handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse3);
			handler.notifyCreditAvailable(inputChannel1);

			assertEquals(1, inputChannel1.getUnannouncedCredit());
			assertEquals(0, inputChannel2.getUnannouncedCredit());

			channel.runPendingTasks();

			// The input channel will not notify credits via un-writable channel
			assertFalse(channel.isWritable());
			assertNull(channel.readOutbound());

			// Flush the buffer to make the channel writable again
			channel.flush();
			assertSame(channelBlockingBuffer, channel.readOutbound());

			// The input channel should notify credits via channel's writability changed event
			assertTrue(channel.isWritable());
			readFromOutbound = channel.readOutbound();
			assertThat(readFromOutbound, instanceOf(AddCredit.class));
			assertEquals(1, ((AddCredit) readFromOutbound).credit);
			assertEquals(0, inputChannel1.getUnannouncedCredit());
			assertEquals(0, inputChannel2.getUnannouncedCredit());

			// no more messages
			assertNull(channel.readOutbound());
		} finally {
			// Release all the buffer resources
			inputGate.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	/**
	 * Verifies that {@link RemoteInputChannel} is enqueued in the pipeline, but {@link AddCredit}
	 * message is not sent actually when this input channel is released.
	 */
	@Test
	public void testNotifyCreditAvailableAfterReleased() throws Exception {
		final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
		final SingleInputGate inputGate = createSingleInputGate();
		final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate);
		inputGate.setInputChannel(inputChannel.getPartitionId().getPartitionId(), inputChannel);
		try {
			final BufferPool bufferPool = networkBufferPool.createBufferPool(6, 6);
			inputGate.setBufferPool(bufferPool);
			final int numExclusiveBuffers = 2;
			inputGate.assignExclusiveSegments(networkBufferPool, numExclusiveBuffers);

			final CreditBasedClientHandler handler = new CreditBasedClientHandler();
			final EmbeddedChannel channel = new EmbeddedChannel(handler);

			// The PartitionRequestClient is tied to PartitionRequestClientHandler currently, so we
			// have to add input channels in CreditBasedClientHandler explicitly
			inputChannel.requestSubpartition(0);
			handler.addInputChannel(inputChannel);

			// Trigger request floating buffers via buffer response to notify credits available
			final BufferResponse bufferResponse = createBufferResponse(
				TestBufferFactory.createBuffer(32), 0, inputChannel.getInputChannelId(), 1);
			handler.channelRead(mock(ChannelHandlerContext.class), bufferResponse);

			assertEquals(2, inputChannel.getUnannouncedCredit());

			// The PartitionRequestClient is tied to PartitionRequestClientHandler currently, so we
			// have to notify credit available in CreditBasedClientHandler explicitly
			handler.notifyCreditAvailable(inputChannel);

			// Release the input channel
			inputGate.releaseAllResources();

			channel.runPendingTasks();

			// It will not notify credits for released input channel
			assertNull(channel.readOutbound());
		} finally {
			// Release all the buffer resources
			inputGate.releaseAllResources();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Creates and returns the single input gate for credit-based testing.
	 *
	 * @return The new created single input gate.
	 */
	private SingleInputGate createSingleInputGate() {
		return new SingleInputGate(
			"InputGate",
			new JobID(),
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED_CREDIT_BASED,
			0,
			1,
			mock(TaskActions.class),
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
	}

	/**
	 * Creates and returns a remote input channel for the specific input gate.
	 *
	 * @param inputGate The input gate owns the created input channel.
	 * @return The new created remote input channel.
	 */
	private RemoteInputChannel createRemoteInputChannel(SingleInputGate inputGate) throws Exception {
		final ConnectionManager connectionManager = mock(ConnectionManager.class);
		final PartitionRequestClient partitionRequestClient = mock(PartitionRequestClient.class);
		when(connectionManager.createPartitionRequestClient(any(ConnectionID.class)))
			.thenReturn(partitionRequestClient);

		return new RemoteInputChannel(
			inputGate,
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			connectionManager,
			0,
			0,
			UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup());
	}

	/**
	 * Returns a deserialized buffer message as it would be received during runtime.
	 */
	private BufferResponse createBufferResponse(
			Buffer buffer,
			int sequenceNumber,
			InputChannelID receivingChannelId,
			int backlog) throws IOException {

		// Mock buffer to serialize
		BufferResponse resp = new BufferResponse(buffer, sequenceNumber, receivingChannelId, backlog);

		ByteBuf serialized = resp.write(UnpooledByteBufAllocator.DEFAULT);

		// Skip general header bytes
		serialized.readBytes(NettyMessage.FRAME_HEADER_LENGTH);

		// Deserialize the bytes again. We have to go this way, because we only partly deserialize
		// the header of the response and wait for a buffer from the buffer pool to copy the payload
		// data into.
		BufferResponse deserialized = BufferResponse.readFrom(serialized);

		return deserialized;
	}
}
