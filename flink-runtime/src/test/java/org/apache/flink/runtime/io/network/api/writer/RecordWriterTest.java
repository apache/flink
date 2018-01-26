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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.io.network.util.TestTaskEvent;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.XORShiftRandom;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
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

/**
 * Tests for the {@link RecordWriter}.
 */
@PrepareForTest({EventSerializer.class})
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

			final TrackingBufferRecycler recycler = new TrackingBufferRecycler();

			final MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(4);

			// Return buffer for first request, but block for all following requests.
			Answer<BufferBuilder> request = new Answer<BufferBuilder>() {
				@Override
				public BufferBuilder answer(InvocationOnMock invocation) throws Throwable {
					sync.countDown();

					if (sync.getCount() == 1) {
						return new BufferBuilder(memorySegment, recycler);
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
			when(bufferProvider.requestBufferBuilderBlocking()).thenAnswer(request);

			ResultPartitionWriter partitionWriter = spy(new RecyclingPartitionWriter(bufferProvider));

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
			verify(bufferProvider, times(2)).requestBufferBuilderBlocking();
			verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());

			// Verify that the written out buffer has only been recycled once
			// (by the partition writer).
			assertEquals(1, recycler.getRecycledMemorySegments().size());
			assertEquals(memorySegment, recycler.getRecycledMemorySegments().get(0));
		}
		finally {
			if (executor != null) {
				executor.shutdown();
			}
		}
	}

	@Test
	public void testClearBuffersAfterExceptionInPartitionWriter() throws Exception {
		NetworkBufferPool buffers = new NetworkBufferPool(1, 1024);
		BufferPool bufferPool = null;

		try {
			bufferPool = buffers.createBufferPool(1, Integer.MAX_VALUE);

			ResultPartitionWriter partitionWriter = mock(ResultPartitionWriter.class);
			when(partitionWriter.getBufferProvider()).thenReturn(checkNotNull(bufferPool));
			when(partitionWriter.getNumberOfSubpartitions()).thenReturn(1);

			// Recycle buffer and throw Exception
			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					Buffer buffer = (Buffer) invocation.getArguments()[0];
					buffer.recycleBuffer();

					throw new ExpectedTestException();
				}
			}).when(partitionWriter).writeBuffer(any(Buffer.class), anyInt());

			RecordWriter<IntValue> recordWriter = new RecordWriter<>(partitionWriter);

			// Validate that memory segment was assigned to recordWriter
			assertEquals(1, buffers.getNumberOfAvailableMemorySegments());
			assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
			recordWriter.emit(new IntValue(0));
			assertEquals(0, buffers.getNumberOfAvailableMemorySegments());
			assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());

			try {
				// Verify that emit correctly clears the buffer. The infinite loop looks
				// dangerous indeed, but the buffer will only be flushed after its full. Adding a
				// manual flush here doesn't test this case (see next).
				for (;;) {
					recordWriter.emit(new IntValue(0));
				}
			}
			catch (ExpectedTestException e) {
				// Verify that the buffer is not part of the record writer state after a failure
				// to flush it out. If the buffer is still part of the record writer state, this
				// will fail, because the buffer has already been recycled. NOTE: The mock
				// partition writer needs to recycle the buffer to correctly test this.
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(1)).writeBuffer(any(Buffer.class), anyInt());
			assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());

			try {
				// Verify that manual flushing correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
				recordWriter.flush();

				Assert.fail("Did not throw expected test Exception");
			}
			catch (ExpectedTestException e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(2)).writeBuffer(any(Buffer.class), anyInt());
			assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());

			try {
				// Verify that broadcast emit correctly clears the buffer.
				recordWriter.broadcastEmit(new IntValue(0));
				assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());

				for (;;) {
					recordWriter.broadcastEmit(new IntValue(0));
				}
			}
			catch (ExpectedTestException e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(3)).writeBuffer(any(Buffer.class), anyInt());
			assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());

			try {
				// Verify that end of super step correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
				recordWriter.broadcastEvent(EndOfSuperstepEvent.INSTANCE);

				Assert.fail("Did not throw expected test Exception");
			}
			catch (ExpectedTestException e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(4)).writeBuffer(any(Buffer.class), anyInt());
			assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());

			try {
				// Verify that broadcasting and event correctly clears the buffer.
				recordWriter.emit(new IntValue(0));
				assertEquals(0, bufferPool.getNumberOfAvailableMemorySegments());
				recordWriter.broadcastEvent(new TestTaskEvent());

				Assert.fail("Did not throw expected test Exception");
			}
			catch (ExpectedTestException e) {
				recordWriter.clearBuffers();
			}

			// Verify expected methods have been called
			verify(partitionWriter, times(5)).writeBuffer(any(Buffer.class), anyInt());
			assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());
		}
		finally {
			if (bufferPool != null) {
				assertEquals(1, bufferPool.getNumberOfAvailableMemorySegments());
				bufferPool.lazyDestroy();
			}

			assertEquals(1, buffers.getNumberOfAvailableMemorySegments());
			buffers.destroy();
		}
	}

	@Test
	public void testSerializerClearedAfterClearBuffers() throws Exception {
		ResultPartitionWriter partitionWriter =
			spy(new RecyclingPartitionWriter(new TestPooledBufferProvider(1, 16)));

		RecordWriter<IntValue> recordWriter = new RecordWriter<>(partitionWriter);

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
		Queue<Buffer>[] queues = new Queue[numChannels];
		for (int i = 0; i < numChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);

		ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = new RecordWriter<>(partitionWriter, new RoundRobin<ByteArrayIO>());
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 919192L, Integer.MAX_VALUE + 18828228L, CheckpointOptions.forCheckpointWithDefaultLocation());

		// No records emitted yet, broadcast should not request a buffer
		writer.broadcastEvent(barrier);

		assertEquals(0, bufferProvider.getNumberOfCreatedBuffers());

		for (int i = 0; i < numChannels; i++) {
			assertEquals(1, queues[i].size());
			BufferOrEvent boe = parseBuffer(queues[i].remove(), i);
			assertTrue(boe.isEvent());
			assertEquals(barrier, boe.getEvent());
			assertEquals(0, queues[i].size());
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
		Queue<Buffer>[] queues = new Queue[numChannels];
		for (int i = 0; i < numChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);

		ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = new RecordWriter<>(partitionWriter, new RoundRobin<ByteArrayIO>());
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 1292L, Integer.MAX_VALUE + 199L, CheckpointOptions.forCheckpointWithDefaultLocation());

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

		assertEquals(4, bufferProvider.getNumberOfCreatedBuffers());

		BufferOrEvent boe;
		assertEquals(2, queues[0].size()); // 1 buffer + 1 event
		assertTrue(parseBuffer(queues[0].remove(), 0).isBuffer());
		assertEquals(3, queues[1].size()); // 2 buffers + 1 event
		assertTrue(parseBuffer(queues[1].remove(), 1).isBuffer());
		assertTrue(parseBuffer(queues[1].remove(), 1).isBuffer());
		assertEquals(2, queues[2].size()); // 1 buffer + 1 event
		assertTrue(parseBuffer(queues[2].remove(), 2).isBuffer());
		assertEquals(1, queues[3].size()); // 0 buffers + 1 event

		// every queue's last element should be the event
		for (int i = 0; i < numChannels; i++) {
			boe = parseBuffer(queues[i].remove(), i);
			assertTrue(boe.isEvent());
			assertEquals(barrier, boe.getEvent());
		}
	}

	/**
	 * Tests that event buffers are properly recycled when broadcasting events
	 * to multiple channels.
	 */
	@Test
	public void testBroadcastEventBufferReferenceCounting() throws Exception {
		Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);

		// Partial mocking of static method...
		PowerMockito
			.stub(PowerMockito.method(EventSerializer.class, "toBuffer"))
			.toReturn(buffer);

		@SuppressWarnings("unchecked")
		ArrayDeque<Buffer>[] queues = new ArrayDeque[] { new ArrayDeque(), new ArrayDeque() };

		ResultPartitionWriter partition =
			new CollectingPartitionWriter(queues, new TestPooledBufferProvider(Integer.MAX_VALUE));
		RecordWriter<?> writer = new RecordWriter<>(partition);

		writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);

		// Verify added to all queues
		assertEquals(1, queues[0].size());
		assertEquals(1, queues[1].size());

		// process all collected events (recycles the buffer)
		for (int i = 0; i < queues.length; i++) {
			assertTrue(parseBuffer(queues[i].remove(), i).isEvent());
		}

		assertTrue(buffer.isRecycled());
	}

	/**
	 * Tests that broadcasted events' buffers are independent (in their (reader) indices) once they
	 * are put into the queue for Netty when broadcasting events to multiple channels.
	 */
	@Test
	public void testBroadcastEventBufferIndependence() throws Exception {
		@SuppressWarnings("unchecked")
		ArrayDeque<Buffer>[] queues =
			new ArrayDeque[]{new ArrayDeque(), new ArrayDeque()};

		ResultPartitionWriter partition =
			new CollectingPartitionWriter(queues, new TestPooledBufferProvider(Integer.MAX_VALUE));
		RecordWriter<?> writer = new RecordWriter<>(partition);

		writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);

		// Verify added to all queues
		assertEquals(1, queues[0].size());
		assertEquals(1, queues[1].size());

		// these two buffers may share the memory but not the indices!
		Buffer buffer1 = queues[0].remove();
		Buffer buffer2 = queues[1].remove();
		assertEquals(0, buffer1.getReaderIndex());
		assertEquals(0, buffer2.getReaderIndex());
		buffer1.setReaderIndex(1);
		assertEquals("Buffer 2 shares the same reader index as buffer 1", 0, buffer2.getReaderIndex());
	}

	/**
	 * Tests that broadcasted records' buffers are independent (in their (reader) indices) once they
	 * are put into the queue for Netty when broadcasting events to multiple channels.
	 */
	@Test
	public void testBroadcastEmitBufferIndependence() throws Exception {
		@SuppressWarnings("unchecked")
		ArrayDeque<Buffer>[] queues =
			new ArrayDeque[]{new ArrayDeque(), new ArrayDeque()};

		ResultPartitionWriter partition =
			new CollectingPartitionWriter(queues, new TestPooledBufferProvider(Integer.MAX_VALUE));
		RecordWriter<IntValue> writer = new RecordWriter<>(partition);

		writer.broadcastEmit(new IntValue(0));
		writer.flush();

		// Verify added to all queues
		assertEquals(1, queues[0].size());
		assertEquals(1, queues[1].size());

		// these two buffers may share the memory but not the indices!
		Buffer buffer1 = queues[0].remove();
		Buffer buffer2 = queues[1].remove();
		assertEquals(0, buffer1.getReaderIndex());
		assertEquals(0, buffer2.getReaderIndex());
		buffer1.setReaderIndex(1);
		assertEquals("Buffer 2 shares the same reader index as buffer 1", 0, buffer2.getReaderIndex());
	}

	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------

	/**
	 * Partition writer that collects the added buffers/events in multiple queue.
	 */
	private static class CollectingPartitionWriter implements ResultPartitionWriter {
		private final Queue<Buffer>[] queues;
		private final BufferProvider bufferProvider;
		private final ResultPartitionID partitionId = new ResultPartitionID();

		/**
		 * Create the partition writer.
		 *
		 * @param queues one queue per outgoing channel
		 * @param bufferProvider buffer provider
		 */
		private CollectingPartitionWriter(Queue<Buffer>[] queues, BufferProvider bufferProvider) {
			this.queues = queues;
			this.bufferProvider = bufferProvider;
		}

		@Override
		public BufferProvider getBufferProvider() {
			return bufferProvider;
		}

		@Override
		public ResultPartitionID getPartitionId() {
			return partitionId;
		}

		@Override
		public int getNumberOfSubpartitions() {
			return queues.length;
		}

		@Override
		public int getNumTargetKeyGroups() {
			return 1;
		}

		@Override
		public void writeBuffer(Buffer buffer, int targetChannel) throws IOException {
			queues[targetChannel].add(buffer);
		}
	}

	private static BufferOrEvent parseBuffer(Buffer buffer, int targetChannel) throws IOException {
		if (buffer.isBuffer()) {
			return new BufferOrEvent(buffer, targetChannel);
		} else {
			// is event:
			AbstractEvent event = EventSerializer.fromBuffer(buffer, RecordWriterTest.class.getClassLoader());
			buffer.recycleBuffer(); // the buffer is not needed anymore
			return new BufferOrEvent(event, targetChannel);
		}
	}

	/**
	 * Partition writer that recycles all received buffers and does no further processing.
	 */
	private static class RecyclingPartitionWriter implements ResultPartitionWriter {
		private final BufferProvider bufferProvider;
		private final ResultPartitionID partitionId = new ResultPartitionID();

		private RecyclingPartitionWriter(BufferProvider bufferProvider) {
			this.bufferProvider = bufferProvider;
		}

		@Override
		public BufferProvider getBufferProvider() {
			return bufferProvider;
		}

		@Override
		public ResultPartitionID getPartitionId() {
			return partitionId;
		}

		@Override
		public int getNumberOfSubpartitions() {
			return 1;
		}

		@Override
		public int getNumTargetKeyGroups() {
			return 1;
		}

		@Override
		public void writeBuffer(Buffer buffer, int targetChannel) throws IOException {
			buffer.recycleBuffer();
		}
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
			in.readFully(bytes);
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

	private static class TrackingBufferRecycler implements BufferRecycler {
		private final ArrayList<MemorySegment> recycledMemorySegments = new ArrayList<>();

		@Override
		public synchronized void recycle(MemorySegment memorySegment) {
			recycledMemorySegments.add(memorySegment);
		}

		public synchronized List<MemorySegment> getRecycledMemorySegments() {
			return recycledMemorySegments;
		}
	}
}
