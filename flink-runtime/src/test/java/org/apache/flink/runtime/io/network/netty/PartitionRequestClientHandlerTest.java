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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestInfiniteBufferProvider;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.util.event.EventListener;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
		when(bufferProvider.addListener(any(EventListener.class))).thenReturn(false);

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		final BufferResponse ReceivedBuffer = createBufferResponse(
				TestBufferFactory.createBuffer(), 0, inputChannel.getInputChannelId());

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(inputChannel);

		client.channelRead(mock(ChannelHandlerContext.class), ReceivedBuffer);
	}

	/**
	 * Tests a fix for FLINK-1761.
	 *
	 * <p> FLINK-1761 discovered an IndexOutOfBoundsException, when receiving buffers of size 0.
	 */
	@Test
	public void testReceiveEmptyBuffer() throws Exception {
		// Minimal mock of a remote input channel
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer());

		final RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(new InputChannelID());
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider);

		// An empty buffer of size 0
		final Buffer emptyBuffer = TestBufferFactory.createBuffer();
		emptyBuffer.setSize(0);

		final BufferResponse receivedBuffer = createBufferResponse(
				emptyBuffer, 0, inputChannel.getInputChannelId());

		final PartitionRequestClientHandler client = new PartitionRequestClientHandler();
		client.addInputChannel(inputChannel);

		// Read the empty buffer
		client.channelRead(mock(ChannelHandlerContext.class), receivedBuffer);

		// This should not throw an exception
		verify(inputChannel, never()).onError(any(Throwable.class));
	}

	/**
	 * Verifies that {@link RemoteInputChannel#onFailedPartitionRequest()} is called when a
	 * {@link PartitionNotFoundException} is received.
	 */
	@Test
	public void testReceivePartitionNotFoundException() throws Exception {
		// Minimal mock of a remote input channel
		final BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBuffer()).thenReturn(TestBufferFactory.createBuffer());

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

	@Test
	public void testCapacityLimitedRemoteInputChannels() throws Exception {
		int capacityLimit = 2;

		ConnectionManager connManager = mock(ConnectionManager.class);
		when(connManager.createPartitionRequestClient(any(ConnectionID.class)))
			.thenReturn(mock(PartitionRequestClient.class));

		BufferProvider bufferProvider = new TestInfiniteBufferProvider();

		SingleInputGate gate = mock(SingleInputGate.class);
		when(gate.getBufferProvider()).thenReturn(bufferProvider);

		RemoteInputChannel inputChannel = new RemoteInputChannel(
			gate,
			0,
			new ResultPartitionID(),
			mock(ConnectionID.class),
			connManager,
			new Tuple2<>(0, 0),
			new UnregisteredTaskMetricsGroup.DummyIOMetricGroup(),
			capacityLimit);

		// Fake request for internal channel logic
		inputChannel.requestSubpartition(0);

		PartitionRequestClientHandler handler = new PartitionRequestClientHandler();
		handler.addInputChannel(inputChannel);

		EmbeddedChannel ch = new EmbeddedChannel(handler);

		int seqNo = 0;

		// Queue buffers until we reach the limit
		Buffer buffer = new Buffer(MemorySegmentFactory.allocateUnpooledSegment(bufferProvider.getMemorySegmentSize()), FreeingBufferRecycler.INSTANCE);
		buffer.retain();
		buffer.retain();

		BufferResponse msg = createBufferResponse(buffer, seqNo++, inputChannel.getInputChannelId());
		ch.writeInbound(msg);

		msg = createBufferResponse(buffer, seqNo++, inputChannel.getInputChannelId());
		ch.writeInbound(msg);

		assertEquals(2, inputChannel.getNumberOfQueuedBuffers());

		// Reached the limit, don't queue this
		msg = createBufferResponse(buffer, seqNo++, inputChannel.getInputChannelId());
		ch.writeInbound(msg);

		// Verify didn't queue and unsubscribed reads
		assertEquals(2, inputChannel.getNumberOfQueuedBuffers());
		assertFalse(ch.config().isAutoRead());

		InputChannel.BufferAndAvailability baa = inputChannel.getNextBuffer();
		assertNotNull(baa);
		assertTrue(baa.moreAvailable());

		baa = inputChannel.getNextBuffer();
		assertNotNull(baa);
		assertFalse(baa.moreAvailable());

		// Run the pending availability callbacks
		ch.runPendingTasks();

		assertEquals(1, inputChannel.getNumberOfQueuedBuffers());
		assertTrue(ch.config().isAutoRead());

		baa = inputChannel.getNextBuffer();
		assertNotNull(baa);
		assertFalse(baa.moreAvailable());
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Returns a deserialized buffer message as it would be received during runtime.
	 */
	private BufferResponse createBufferResponse(
			Buffer buffer,
			int sequenceNumber,
			InputChannelID receivingChannelId) throws IOException {

		// Mock buffer to serialize
		BufferResponse resp = new BufferResponse(buffer, sequenceNumber, receivingChannelId);

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
}
