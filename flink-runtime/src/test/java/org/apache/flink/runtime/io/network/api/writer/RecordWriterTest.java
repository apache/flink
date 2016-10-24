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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.testutils.DiscardingRecycler;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.XORShiftRandom;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PrepareForTest(ResultPartitionWriter.class)
@RunWith(PowerMockRunner.class)
public class RecordWriterTest {

	// ---------------------------------------------------------------------------------------------
	// Resource release tests
	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests a fix for FLINK-2089.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-2089">FLINK-2089</a>
	 */
	@Test
	public void testClearBuffersAfterInterruptDuringBlockingBufferRequest() throws Exception {
		ExecutorService executor = null;

		try {
			executor = Executors.newSingleThreadExecutor();

			final CountDownLatch sync = new CountDownLatch(2);

			final Buffer buffer = spy(TestBufferFactory.createBuffer(4));

			// Return buffer for first request, but block for all following requests.
			Answer<Buffer> request = new Answer<Buffer>() {
				@Override
				public Buffer answer(InvocationOnMock invocation) throws Throwable {
					sync.countDown();

					if (sync.getCount() == 1) {
						return buffer;
					}

					final Object o = new Object();
					synchronized (o) {
						while (true) {
							o.wait();
						}
					}
				}
			};

			BufferProvider bufferProvider = mock(BufferProvider.class);
			when(bufferProvider.requestBufferBlocking()).thenAnswer(request);

			ResultPartitionWriter partitionWriter = createResultPartitionWriter(bufferProvider);

			final RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

			Future<?> result = executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					IntValue val = new IntValue(0);

					try {
						recordWriter.emit(val);
						recordWriter.flush();

						recordWriter.emit(val);
					}
					catch (InterruptedException e) {
						recordWriter.clearBuffers();
					}

					return null;
				}
			});

			sync.await();

			// Interrupt the Thread.
			//
			// The second emit call requests a new buffer and blocks the thread.
			// When interrupting the thread at this point, clearing the buffers
			// should not recycle any buffer.
			result.cancel(true);

			recordWriter.clearBuffers();

			// Verify that buffer have been requested, but only one has been written out.
			verify(bufferProvider, times(2)).requestBufferBlocking();
			verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());

			// Verify that the written out buffer has only been recycled once
			// (by the partition writer).
			assertTrue("Buffer not recycled.", buffer.isRecycled());
			verify(buffer, times(1)).recycle();
		}
		finally {
			if (executor != null) {
				executor.shutdown();
			}
		}
	}

	@Test
	public void testClearBuffersAfterExceptionInPartitionWriter() throws Exception {
		NetworkBufferPool buffers = null;
		BufferPool bufferPool = null;

		try {
			buffers = new NetworkBufferPool(1, 1024, MemoryType.HEAP);
			bufferPool = spy(buffers.createBufferPool(1, true));

			ResultPartitionWriter partitionWriter = mock(ResultPartitionWriter.class);
			when(partitionWriter.getBufferProvider()).thenReturn(checkNotNull(bufferPool));
			when(partitionWriter.getNumberOfOutputChannels()).thenReturn(1);

			// Recycle buffer and throw Exception
			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					Buffer buffer = (Buffer) invocation.getArguments()[0];
					buffer.recycle();

					throw new RuntimeException("Expected test Exception");
				}
			}).when(partitionWriter).writeBuffer(any(Buffer.class), anyInt());

			RecordWriter<IntValue> recordWriter = new RecordWriter<>(partitionWriter);

			try {
				// Verify that emit correctly clears the buffer. The infinite loop looks
				// dangerous indeed, but the buffer will only be flushed after its full. Adding a
				// manual flush here doesn't test this case (see next).
				for (;;) {
					recordWriter.emit(new IntValue(0));
				}
			}
			catch (Exception e) {
				// Verify that the buffer is not part of the record writer state after a failure
				// to flush it out. If the buffer is still part of the record writer state, this
				// will fail, because the buffer has already been recycled. NOTE: The mock
				// partition writer needs to recycle the buffer to correctly test this.
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());
			verify(bufferPool, times(1)).requestBufferBlocking();

			try {
				// Verify that manual flushing correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				recordWriter.flush();

				Assert.fail("Did not throw expected test Exception");
			}
			catch (Exception e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(2)).writeBuffer(any(Buffer.class), anyInt());
			verify(bufferPool, times(2)).requestBufferBlocking();

			try {
				// Verify that broadcast emit correctly clears the buffer.
				for (;;) {
					recordWriter.broadcastEmit(new IntValue(0));
				}
			}
			catch (Exception e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(3)).writeBuffer(any(Buffer.class), anyInt());
			verify(bufferPool, times(3)).requestBufferBlocking();

			try {
				// Verify that end of super step correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				recordWriter.sendEndOfSuperstep();

				Assert.fail("Did not throw expected test Exception");
			}
			catch (Exception e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(4)).writeBuffer(any(Buffer.class), anyInt());
			verify(bufferPool, times(4)).requestBufferBlocking();

			try {
				// Verify that broadcasting and event correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				recordWriter.broadcastEvent(new TestTaskEvent());

				Assert.fail("Did not throw expected test Exception");
			}
			catch (Exception e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(5)).writeBuffer(any(Buffer.class), anyInt());
			verify(bufferPool, times(5)).requestBufferBlocking();
		}
		finally {
			if (bufferPool != null) {
				assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());
				bufferPool.lazyDestroy();
			}

			if (buffers != null) {
				assertEquals(1, buffers.getNumberOfAvailableMemorySegments());
				buffers.destroy();
			}
		}
	}

	@Test
	public void testSerializerClearedAfterClearBuffers() throws Exception {

		final Buffer buffer = TestBufferFactory.createBuffer(16);

		ResultPartitionWriter partitionWriter = createResultPartitionWriter(
				createBufferProvider(buffer));

		RecordWriter<IntValue> recordWriter = new RecordWriter<IntValue>(partitionWriter);

		// Fill a buffer, but don't write it out.
		recordWriter.emit(new IntValue(0));
		verify(partitionWriter, never()).writeBuffer(any(Buffer.class), anyInt());

		// Clear all buffers.
		recordWriter.clearBuffers();

		// This should not throw an Exception iff the serializer state
		// has been cleared as expected.
		recordWriter.flush();
	}

	/**
	 * Tests broadcasting events when no records have been emitted yet.
	 */
	@Test
	public void testBroadcastEventNoRecords() throws Exception {
		int numChannels = 4;
		int bufferSize = 32;

		@SuppressWarnings("unchecked")
		Queue<BufferOrEvent>[] queues = new Queue[numChannels];
		for (int i = 0; i < numChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		BufferProvider bufferProvider = createBufferProvider(bufferSize);

		ResultPartitionWriter partitionWriter = createCollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = new RecordWriter<>(partitionWriter, new RoundRobin<ByteArrayIO>());
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 919192L, Integer.MAX_VALUE + 18828228L);

		// No records emitted yet, broadcast should not request a buffer
		writer.broadcastEvent(barrier);

		verify(bufferProvider, times(0)).requestBufferBlocking();

		for (Queue<BufferOrEvent> queue : queues) {
			assertEquals(1, queue.size());
			BufferOrEvent boe = queue.remove();
			assertTrue(boe.isEvent());
			assertEquals(barrier, boe.getEvent());
		}
	}

	/**
	 * Tests broadcasting events when records have been emitted. The emitted
	 * records cover all three {@link SerializationResult} types.
	 */
	@Test
	public void testBroadcastEventMixedRecords() throws Exception {
		Random rand = new XORShiftRandom();
		int numChannels = 4;
		int bufferSize = 32;
		int lenBytes = 4; // serialized length

		@SuppressWarnings("unchecked")
		Queue<BufferOrEvent>[] queues = new Queue[numChannels];
		for (int i = 0; i < numChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		BufferProvider bufferProvider = createBufferProvider(bufferSize);

		ResultPartitionWriter partitionWriter = createCollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = new RecordWriter<>(partitionWriter, new RoundRobin<ByteArrayIO>());
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 1292L, Integer.MAX_VALUE + 199L);

		// Emit records on some channels first (requesting buffers), then
		// broadcast the event. The record buffers should be emitted first, then
		// the event. After the event, no new buffer should be requested.

		// (i) Smaller than the buffer size (single buffer request => 1)
		byte[] bytes = new byte[bufferSize / 2];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (ii) Larger than the buffer size (two buffer requests => 1 + 2)
		bytes = new byte[bufferSize + 1];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (iii) Exactly the buffer size (single buffer request => 1 + 2 + 1)
		bytes = new byte[bufferSize - lenBytes];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (iv) Nothing on the 4th channel (no buffer request => 1 + 2 + 1 + 0 = 4)

		// (v) Broadcast the event
		writer.broadcastEvent(barrier);

		verify(bufferProvider, times(4)).requestBufferBlocking();

		assertEquals(2, queues[0].size()); // 1 buffer + 1 event
		assertEquals(3, queues[1].size()); // 2 buffers + 1 event
		assertEquals(2, queues[2].size()); // 1 buffer + 1 event
		assertEquals(1, queues[3].size()); // 0 buffers + 1 event
	}

	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------

	/**
	 * Creates a mock partition writer that collects the added buffers/events.
	 *
	 * <p>This much mocking should not be necessary with better designed
	 * interfaces. Refactoring this will take too much time now though, hence
	 * the mocking. Ideally, we will refactor all of this mess in order to make
	 * our lives easier and test it better.
	 */
	private ResultPartitionWriter createCollectingPartitionWriter(
			final Queue<BufferOrEvent>[] queues,
			BufferProvider bufferProvider) throws IOException {

		int numChannels = queues.length;

		ResultPartitionWriter partitionWriter = mock(ResultPartitionWriter.class);
		when(partitionWriter.getBufferProvider()).thenReturn(checkNotNull(bufferProvider));
		when(partitionWriter.getNumberOfOutputChannels()).thenReturn(numChannels);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				Buffer buffer = (Buffer) invocationOnMock.getArguments()[0];
				Integer targetChannel = (Integer) invocationOnMock.getArguments()[1];
				queues[targetChannel].add(new BufferOrEvent(buffer, targetChannel));
				return null;
			}
		}).when(partitionWriter).writeBuffer(any(Buffer.class), anyInt());

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				AbstractEvent event = (AbstractEvent) invocationOnMock.getArguments()[0];
				Integer targetChannel = (Integer) invocationOnMock.getArguments()[1];
				queues[targetChannel].add(new BufferOrEvent(event, targetChannel));
				return null;
			}
		}).when(partitionWriter).writeEvent(any(AbstractEvent.class), anyInt());

		return partitionWriter;
	}

	private BufferProvider createBufferProvider(final int bufferSize)
			throws IOException, InterruptedException {

		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(bufferProvider.requestBufferBlocking()).thenAnswer(
				new Answer<Buffer>() {
					@Override
					public Buffer answer(InvocationOnMock invocationOnMock) throws Throwable {
						MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
						Buffer buffer = new Buffer(segment, DiscardingRecycler.INSTANCE);
						return buffer;
					}
				}
		);

		return bufferProvider;
	}

	private BufferProvider createBufferProvider(Buffer... buffers)
			throws IOException, InterruptedException {

		BufferProvider bufferProvider = mock(BufferProvider.class);

		for (int i = 0; i < buffers.length; i++) {
			when(bufferProvider.requestBufferBlocking()).thenReturn(buffers[i]);
		}

		return bufferProvider;
	}

	private ResultPartitionWriter createResultPartitionWriter(BufferProvider bufferProvider)
			throws IOException {

		ResultPartitionWriter partitionWriter = mock(ResultPartitionWriter.class);
		when(partitionWriter.getBufferProvider()).thenReturn(checkNotNull(bufferProvider));
		when(partitionWriter.getNumberOfOutputChannels()).thenReturn(1);

		// Recycle each written buffer.
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				((Buffer) invocation.getArguments()[0]).recycle();

				return null;
			}
		}).when(partitionWriter).writeBuffer(any(Buffer.class), anyInt());

		return partitionWriter;
	}

	private static class ByteArrayIO implements IOReadableWritable {

		private final byte[] bytes;

		public ByteArrayIO(byte[] bytes) {
			this.bytes = bytes;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.write(bytes);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			in.read(bytes);
		}
	}

	/**
	 * RoundRobin channel selector starting at 0 ({@link RoundRobinChannelSelector} starts at 1).
	 */
	private static class RoundRobin<T extends IOReadableWritable> implements ChannelSelector<T> {

		private int[] nextChannel = new int[] { -1 };

		@Override
		public int[] selectChannels(final T record, final int numberOfOutputChannels) {
			nextChannel[0] = (nextChannel[0] + 1) % numberOfOutputChannels;
			return nextChannel;
		}
	}
}
