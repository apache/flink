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

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.testutils.DiscardingRecycler;
import org.apache.flink.runtime.util.event.EventListener;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
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

	/**
	 * Tests that an unsuccessful message decode call for a staged message
	 * does not leave the channel with auto read set to false.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAutoReadAfterUnsuccessfulStagedMessage() throws Exception {
		PartitionRequestClientHandler handler = new PartitionRequestClientHandler();
		EmbeddedChannel channel = new EmbeddedChannel(handler);

		final AtomicReference<EventListener<Buffer>> listener = new AtomicReference<>();

		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.addListener(any(EventListener.class))).thenAnswer(new Answer<Boolean>() {
			@Override
			@SuppressWarnings("unchecked")
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				listener.set((EventListener<Buffer>) invocation.getArguments()[0]);
				return true;
			}
		});

		when(bufferProvider.requestBuffer()).thenReturn(null);

		InputChannelID channelId = new InputChannelID(0, 0);
		RemoteInputChannel inputChannel = mock(RemoteInputChannel.class);
		when(inputChannel.getInputChannelId()).thenReturn(channelId);

		// The 3rd staged msg has a null buffer provider
		when(inputChannel.getBufferProvider()).thenReturn(bufferProvider, bufferProvider, null);

		handler.addInputChannel(inputChannel);

		BufferResponse msg = createBufferResponse(createBuffer(true), 0, channelId);

		// Write 1st buffer msg. No buffer is available, therefore the buffer
		// should be staged and auto read should be set to false.
		assertTrue(channel.config().isAutoRead());
		channel.writeInbound(msg);

		// No buffer available, auto read false
		assertFalse(channel.config().isAutoRead());

		// Write more buffers... all staged.
		msg = createBufferResponse(createBuffer(true), 1, channelId);
		channel.writeInbound(msg);

		msg = createBufferResponse(createBuffer(true), 2, channelId);
		channel.writeInbound(msg);

		// Notify about buffer => handle 1st msg
		Buffer availableBuffer = createBuffer(false);
		listener.get().onEvent(availableBuffer);

		// Start processing of staged buffers (in run pending tasks). Make
		// sure that the buffer provider acts like it's destroyed.
		when(bufferProvider.addListener(any(EventListener.class))).thenReturn(false);
		when(bufferProvider.isDestroyed()).thenReturn(true);

		// Execute all tasks that are scheduled in the event loop. Further
		// eventLoop().execute() calls are directly executed, if they are
		// called in the scope of this call.
		channel.runPendingTasks();

		assertTrue(channel.config().isAutoRead());
	}

	// ---------------------------------------------------------------------------------------------

	private static Buffer createBuffer(boolean fill) {
		MemorySegment segment = HeapMemorySegment.FACTORY.allocateUnpooledSegment(1024, null);
		if (fill) {
			for (int i = 0; i < 1024; i++) {
				segment.put(i, (byte) i);
			}
		}
		return new Buffer(segment, DiscardingRecycler.INSTANCE, true);
	}

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
