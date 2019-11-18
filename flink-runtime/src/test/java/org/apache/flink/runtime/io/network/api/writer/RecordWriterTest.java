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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.DeserializationUtils;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.taskmanager.ConsumableNotifyingResultPartitionWriterDecorator;
import org.apache.flink.runtime.taskmanager.NoOpTaskActions;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.XORShiftRandom;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.AvailabilityProvider.AVAILABLE;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RecordWriter}.
 */
public class RecordWriterTest {

	private final boolean isBroadcastWriter;

	public RecordWriterTest() {
		this(false);
	}

	RecordWriterTest(boolean isBroadcastWriter) {
		this.isBroadcastWriter = isBroadcastWriter;
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

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

			ResultPartitionWriter partitionWriter = new RecyclingPartitionWriter(bufferProvider);

			final RecordWriter<IntValue> recordWriter = createRecordWriter(partitionWriter);

			Future<?> result = executor.submit(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					IntValue val = new IntValue(0);

					try {
						recordWriter.emit(val);
						recordWriter.flushAll();

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

			// Verify that buffer have been requested twice
			verify(bufferProvider, times(2)).requestBufferBuilderBlocking();

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
	public void testSerializerClearedAfterClearBuffers() throws Exception {
		ResultPartitionWriter partitionWriter =
			spy(new RecyclingPartitionWriter(new TestPooledBufferProvider(1, 16)));

		RecordWriter<IntValue> recordWriter = createRecordWriter(partitionWriter);

		// Fill a buffer, but don't write it out.
		recordWriter.emit(new IntValue(0));

		// Clear all buffers.
		recordWriter.clearBuffers();

		// This should not throw an Exception iff the serializer state
		// has been cleared as expected.
		recordWriter.flushAll();
	}

	/**
	 * Tests broadcasting events when no records have been emitted yet.
	 */
	@Test
	public void testBroadcastEventNoRecords() throws Exception {
		int numberOfChannels = 4;
		int bufferSize = 32;

		@SuppressWarnings("unchecked")
		Queue<BufferConsumer>[] queues = new Queue[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);

		ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = createRecordWriter(partitionWriter);
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 919192L, Integer.MAX_VALUE + 18828228L, CheckpointOptions.forCheckpointWithDefaultLocation());

		// No records emitted yet, broadcast should not request a buffer
		writer.broadcastEvent(barrier);

		assertEquals(0, bufferProvider.getNumberOfCreatedBuffers());

		for (int i = 0; i < numberOfChannels; i++) {
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
		int numberOfChannels = 4;
		int bufferSize = 32;
		int lenBytes = 4; // serialized length

		@SuppressWarnings("unchecked")
		Queue<BufferConsumer>[] queues = new Queue[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);

		ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		RecordWriter<ByteArrayIO> writer = createRecordWriter(partitionWriter);
		CheckpointBarrier barrier = new CheckpointBarrier(Integer.MAX_VALUE + 1292L, Integer.MAX_VALUE + 199L, CheckpointOptions.forCheckpointWithDefaultLocation());

		// Emit records on some channels first (requesting buffers), then
		// broadcast the event. The record buffers should be emitted first, then
		// the event. After the event, no new buffer should be requested.

		// (i) Smaller than the buffer size
		byte[] bytes = new byte[bufferSize / 2];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (ii) Larger than the buffer size
		bytes = new byte[bufferSize + 1];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (iii) Exactly the buffer size
		bytes = new byte[bufferSize - lenBytes];
		rand.nextBytes(bytes);

		writer.emit(new ByteArrayIO(bytes));

		// (iv) Broadcast the event
		writer.broadcastEvent(barrier);

		if (isBroadcastWriter) {
			assertEquals(3, bufferProvider.getNumberOfCreatedBuffers());

			for (int i = 0; i < numberOfChannels; i++) {
				assertEquals(4, queues[i].size()); // 3 buffer + 1 event

				for (int j = 0; j < 3; j++) {
					assertTrue(parseBuffer(queues[i].remove(), 0).isBuffer());
				}

				BufferOrEvent boe = parseBuffer(queues[i].remove(), i);
				assertTrue(boe.isEvent());
				assertEquals(barrier, boe.getEvent());
			}
		} else {
			assertEquals(4, bufferProvider.getNumberOfCreatedBuffers());

			assertEquals(2, queues[0].size()); // 1 buffer + 1 event
			assertTrue(parseBuffer(queues[0].remove(), 0).isBuffer());
			assertEquals(3, queues[1].size()); // 2 buffers + 1 event
			assertTrue(parseBuffer(queues[1].remove(), 1).isBuffer());
			assertTrue(parseBuffer(queues[1].remove(), 1).isBuffer());
			assertEquals(2, queues[2].size()); // 1 buffer + 1 event
			assertTrue(parseBuffer(queues[2].remove(), 2).isBuffer());
			assertEquals(1, queues[3].size()); // 0 buffers + 1 event

			// every queue's last element should be the event
			for (int i = 0; i < numberOfChannels; i++) {
				BufferOrEvent boe = parseBuffer(queues[i].remove(), i);
				assertTrue(boe.isEvent());
				assertEquals(barrier, boe.getEvent());
			}
		}
	}

	/**
	 * Tests that event buffers are properly recycled when broadcasting events
	 * to multiple channels.
	 */
	@Test
	public void testBroadcastEventBufferReferenceCounting() throws Exception {

		@SuppressWarnings("unchecked")
		ArrayDeque<BufferConsumer>[] queues = new ArrayDeque[] { new ArrayDeque(), new ArrayDeque() };

		ResultPartitionWriter partition =
			new CollectingPartitionWriter(queues, new TestPooledBufferProvider(Integer.MAX_VALUE));
		RecordWriter<?> writer = createRecordWriter(partition);

		writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);

		// Verify added to all queues
		assertEquals(1, queues[0].size());
		assertEquals(1, queues[1].size());

		// get references to buffer consumers (copies from the original event buffer consumer)
		BufferConsumer bufferConsumer1 = queues[0].getFirst();
		BufferConsumer bufferConsumer2 = queues[1].getFirst();

		// process all collected events (recycles the buffer)
		for (int i = 0; i < queues.length; i++) {
			assertTrue(parseBuffer(queues[i].remove(), i).isEvent());
		}

		assertTrue(bufferConsumer1.isRecycled());
		assertTrue(bufferConsumer2.isRecycled());
	}

	/**
	 * Tests that broadcasted events' buffers are independent (in their (reader) indices) once they
	 * are put into the queue for Netty when broadcasting events to multiple channels.
	 */
	@Test
	public void testBroadcastEventBufferIndependence() throws Exception {
		verifyBroadcastBufferOrEventIndependence(true);
	}

	/**
	 * Tests that broadcasted records' buffers are independent (in their (reader) indices) once they
	 * are put into the queue for Netty when broadcasting events to multiple channels.
	 */
	@Test
	public void testBroadcastEmitBufferIndependence() throws Exception {
		verifyBroadcastBufferOrEventIndependence(false);
	}

	/**
	 * Tests that records are broadcast via {@link RecordWriter#broadcastEmit(IOReadableWritable)}.
	 */
	@Test
	public void testBroadcastEmitRecord() throws Exception {
		final int numberOfChannels = 4;
		final int bufferSize = 32;
		final int numValues = 8;
		final int serializationLength = 4;

		@SuppressWarnings("unchecked")
		final Queue<BufferConsumer>[] queues = new Queue[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		final TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);
		final ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		final RecordWriter<SerializationTestType> writer = createRecordWriter(partitionWriter);
		final RecordDeserializer<SerializationTestType> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(
			new String[]{ tempFolder.getRoot().getAbsolutePath() });

		final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<>();
		final Iterable<SerializationTestType> records = Util.randomRecords(numValues, SerializationTestTypeFactory.INT);
		for (SerializationTestType record : records) {
			serializedRecords.add(record);
			writer.broadcastEmit(record);
		}

		final int numRequiredBuffers = numValues / (bufferSize / (4 + serializationLength));
		if (isBroadcastWriter) {
			assertEquals(numRequiredBuffers, bufferProvider.getNumberOfCreatedBuffers());
		} else {
			assertEquals(numRequiredBuffers * numberOfChannels, bufferProvider.getNumberOfCreatedBuffers());
		}

		for (int i = 0; i < numberOfChannels; i++) {
			assertEquals(numRequiredBuffers, queues[i].size());
			verifyDeserializationResults(queues[i], deserializer, serializedRecords.clone(), numRequiredBuffers, numValues);
		}
	}

	/**
	 * Tests that the RecordWriter is available iif the respective LocalBufferPool has at-least one available buffer.
	 */
	@Test
	public void testIsAvailableOrNot() throws Exception {
		// setup
		final NetworkBufferPool globalPool = new NetworkBufferPool(10, 128, 2);
		final BufferPool localPool = globalPool.createBufferPool(1, 1);
		final ResultPartitionWriter resultPartition = new ResultPartitionBuilder()
			.setBufferPoolFactory(p -> localPool)
			.build();
		resultPartition.setup();
		final ResultPartitionWriter partitionWrapper = new ConsumableNotifyingResultPartitionWriterDecorator(
			new NoOpTaskActions(),
			new JobID(),
			resultPartition,
			new NoOpResultPartitionConsumableNotifier());
		final RecordWriter recordWriter = createRecordWriter(partitionWrapper);

		try {
			// record writer is available because of initial available global pool
			assertTrue(recordWriter.getAvailableFuture().isDone());

			// request one buffer from the local pool to make it unavailable afterwards
			final BufferBuilder bufferBuilder = resultPartition.getBufferBuilder();
			assertNotNull(bufferBuilder);
			assertFalse(recordWriter.getAvailableFuture().isDone());

			// recycle the buffer to make the local pool available again
			final Buffer buffer = BufferBuilderTestUtils.buildSingleBuffer(bufferBuilder);
			buffer.recycleBuffer();
			assertTrue(recordWriter.getAvailableFuture().isDone());
			assertEquals(recordWriter.AVAILABLE, recordWriter.getAvailableFuture());
		} finally {
			localPool.lazyDestroy();
			globalPool.destroy();
		}
	}

	private void verifyBroadcastBufferOrEventIndependence(boolean broadcastEvent) throws Exception {
		@SuppressWarnings("unchecked")
		ArrayDeque<BufferConsumer>[] queues = new ArrayDeque[]{new ArrayDeque(), new ArrayDeque()};

		ResultPartitionWriter partition =
			new CollectingPartitionWriter(queues, new TestPooledBufferProvider(Integer.MAX_VALUE));
		RecordWriter<IntValue> writer = createRecordWriter(partition);

		if (broadcastEvent) {
			writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);
		} else {
			writer.broadcastEmit(new IntValue(0));
		}

		// verify added to all queues
		assertEquals(1, queues[0].size());
		assertEquals(1, queues[1].size());

		// these two buffers may share the memory but not the indices!
		Buffer buffer1 = buildSingleBuffer(queues[0].remove());
		Buffer buffer2 = buildSingleBuffer(queues[1].remove());
		assertEquals(0, buffer1.getReaderIndex());
		assertEquals(0, buffer2.getReaderIndex());
		buffer1.setReaderIndex(1);
		assertEquals("Buffer 2 shares the same reader index as buffer 1", 0, buffer2.getReaderIndex());
	}

	protected void verifyDeserializationResults(
			Queue<BufferConsumer> queue,
			RecordDeserializer<SerializationTestType> deserializer,
			ArrayDeque<SerializationTestType> expectedRecords,
			int numRequiredBuffers,
			int numValues) throws Exception {
		int assertRecords = 0;
		for (int j = 0; j < numRequiredBuffers; j++) {
			Buffer buffer = buildSingleBuffer(queue.remove());
			deserializer.setNextBuffer(buffer);

			assertRecords += DeserializationUtils.deserializeRecords(expectedRecords, deserializer);
		}
		Assert.assertEquals(numValues, assertRecords);
	}

	/**
	 * Creates the {@link RecordWriter} instance based on whether it is a broadcast writer.
	 */
	private RecordWriter createRecordWriter(ResultPartitionWriter writer) {
		if (isBroadcastWriter) {
			return new RecordWriterBuilder()
				.setChannelSelector(new OutputEmitter(ShipStrategyType.BROADCAST, 0))
				.build(writer);
		} else {
			return new RecordWriterBuilder().build(writer);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------

	/**
	 * Partition writer that collects the added buffers/events in multiple queue.
	 */
	static class CollectingPartitionWriter implements ResultPartitionWriter {
		private final Queue<BufferConsumer>[] queues;
		private final BufferProvider bufferProvider;
		private final ResultPartitionID partitionId = new ResultPartitionID();

		/**
		 * Create the partition writer.
		 *
		 * @param queues one queue per outgoing channel
		 * @param bufferProvider buffer provider
		 */
		CollectingPartitionWriter(Queue<BufferConsumer>[] queues, BufferProvider bufferProvider) {
			this.queues = queues;
			this.bufferProvider = bufferProvider;
		}

		@Override
		public void setup() {
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
		public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
			return bufferProvider.requestBufferBuilderBlocking();
		}

		@Override
		public boolean addBufferConsumer(BufferConsumer buffer, int targetChannel) throws IOException {
			return queues[targetChannel].add(buffer);
		}

		@Override
		public void flushAll() {
		}

		@Override
		public void flush(int subpartitionIndex) {
		}

		@Override
		public void fail(@Nullable Throwable throwable) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void finish() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return AVAILABLE;
		}

		@Override
		public void close() {
		}
	}

	static BufferOrEvent parseBuffer(BufferConsumer bufferConsumer, int targetChannel) throws IOException {
		Buffer buffer = buildSingleBuffer(bufferConsumer);
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
		public void setup() {
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
		public BufferBuilder getBufferBuilder() throws IOException, InterruptedException {
			return bufferProvider.requestBufferBuilderBlocking();
		}

		@Override
		public boolean addBufferConsumer(BufferConsumer bufferConsumer, int targetChannel) throws IOException {
			bufferConsumer.close();
			return true;
		}

		@Override
		public void flushAll() {
		}

		@Override
		public void flush(int subpartitionIndex) {
		}

		@Override
		public void fail(@Nullable Throwable throwable) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void finish() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CompletableFuture<?> getAvailableFuture() {
			return AVAILABLE;
		}

		@Override
		public void close() {
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
