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

import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link SingleRecordWriter} and {@link MultipleRecordWriters}.
 */
public class RecordWriterDelegateTest extends TestLogger {

	private static final int numberOfBuffers = 10;

	private static final int memorySegmentSize = 128;

	private static final int numberOfSegmentsToRequest = 2;

	private NetworkBufferPool globalPool;

	@Before
	public void setup() {
		globalPool = new NetworkBufferPool(numberOfBuffers, memorySegmentSize);
	}

	@After
	public void teardown() {
		globalPool.destroyAllBufferPools();
		globalPool.destroy();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSingleRecordWriterAvailability() throws Exception {
		final RecordWriter recordWriter = createRecordWriter(globalPool);
		final RecordWriterDelegate writerDelegate = new SingleRecordWriter(recordWriter);

		assertEquals(recordWriter, writerDelegate.getRecordWriter(0));
		verifyAvailability(writerDelegate);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleRecordWritersAvailability() throws Exception {
		// setup
		final int numRecordWriters = 2;
		final List<RecordWriter> recordWriters = new ArrayList<>(numRecordWriters);

		for (int i = 0; i < numRecordWriters; i++) {
			recordWriters.add(createRecordWriter(globalPool));
		}

		RecordWriterDelegate writerDelegate = new MultipleRecordWriters(recordWriters);
		for (int i = 0; i < numRecordWriters; i++) {
			assertEquals(recordWriters.get(i), writerDelegate.getRecordWriter(i));
		}

		verifyAvailability(writerDelegate);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSingleRecordWriterBroadcastEvent() throws Exception {
		// setup
		final ArrayDeque<BufferConsumer>[] queues = new ArrayDeque[] { new ArrayDeque(), new ArrayDeque() };
		final RecordWriter recordWriter = createRecordWriter(queues);
		final RecordWriterDelegate writerDelegate = new SingleRecordWriter(recordWriter);

		verifyBroadcastEvent(writerDelegate, queues, 1);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleRecordWritersBroadcastEvent() throws Exception {
		// setup
		final int numRecordWriters = 2;
		final List<RecordWriter> recordWriters = new ArrayList<>(numRecordWriters);
		final ArrayDeque<BufferConsumer>[] queues = new ArrayDeque[] { new ArrayDeque(), new ArrayDeque() };

		for (int i = 0; i < numRecordWriters; i++) {
			recordWriters.add(createRecordWriter(queues));
		}
		final RecordWriterDelegate writerDelegate = new MultipleRecordWriters(recordWriters);

		verifyBroadcastEvent(writerDelegate, queues, numRecordWriters);
	}

	private RecordWriter createRecordWriter(NetworkBufferPool globalPool) throws Exception {
		final BufferPool localPool = globalPool.createBufferPool(1, 1, null, 1, Integer.MAX_VALUE);
		final ResultPartitionWriter partition = new ResultPartitionBuilder()
			.setBufferPoolFactory(p -> localPool)
			.build();
		partition.setup();

		return new RecordWriterBuilder().build(partition);
	}

	private RecordWriter createRecordWriter(ArrayDeque<BufferConsumer>[] queues) {
		final ResultPartitionWriter partition = new RecordWriterTest.CollectingPartitionWriter(
			queues,
			new TestPooledBufferProvider(1));

		return new RecordWriterBuilder().build(partition);
	}

	private void verifyAvailability(RecordWriterDelegate writerDelegate) throws Exception {
		// writer is available at the beginning
		assertTrue(writerDelegate.isAvailable());
		assertTrue(writerDelegate.getAvailableFuture().isDone());

		// request one buffer from the local pool to make it unavailable
		RecordWriter recordWriter = writerDelegate.getRecordWriter(0);
		final BufferBuilder bufferBuilder = checkNotNull(recordWriter.getBufferBuilder(0));
		assertFalse(writerDelegate.isAvailable());
		CompletableFuture future = writerDelegate.getAvailableFuture();
		assertFalse(future.isDone());

		// recycle the buffer to make the local pool available again
		BufferBuilderTestUtils.fillBufferBuilder(bufferBuilder, 1).finish();
		ResultSubpartitionView readView = recordWriter.getTargetPartition().createSubpartitionView(0, new NoOpBufferAvailablityListener());
		Buffer buffer = readView.getNextBuffer().buffer();

		buffer.recycleBuffer();
		assertTrue(future.isDone());
		assertTrue(writerDelegate.isAvailable());
		assertTrue(writerDelegate.getAvailableFuture().isDone());
	}

	private void verifyBroadcastEvent(
			RecordWriterDelegate writerDelegate,
			ArrayDeque<BufferConsumer>[] queues,
			int numRecordWriters) throws Exception {

		final CancelCheckpointMarker message = new CancelCheckpointMarker(1);
		writerDelegate.broadcastEvent(message);

		// verify the added messages in all the queues
		for (int i = 0; i < queues.length; i++) {
			assertEquals(numRecordWriters, queues[i].size());

			for (int j = 0; j < numRecordWriters; j++) {
				BufferOrEvent boe = RecordWriterTest.parseBuffer(queues[i].remove(), i);
				assertTrue(boe.isEvent());
				assertEquals(message, boe.getEvent());
			}
		}
	}
}
