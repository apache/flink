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
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.util.TestPooledBufferProvider;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link BroadcastRecordWriter}.
 */
public class BroadcastRecordWriterTest extends RecordWriterTest {

	public BroadcastRecordWriterTest() {
		super(true);
	}

	/**
	 * Tests the number of requested buffers and results are correct in the case of switching
	 * modes between {@link BroadcastRecordWriter#broadcastEmit(IOReadableWritable)} and
	 * {@link BroadcastRecordWriter#randomEmit(IOReadableWritable)}.
	 */
	@Test
	public void testBroadcastMixedRandomEmitRecord() throws Exception {
		final int numberOfChannels = 4;
		final int numberOfRecords = 8;
		final int bufferSize = 32;

		@SuppressWarnings("unchecked")
		final Queue<BufferConsumer>[] queues = new Queue[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++) {
			queues[i] = new ArrayDeque<>();
		}

		final TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(Integer.MAX_VALUE, bufferSize);
		final ResultPartitionWriter partitionWriter = new CollectingPartitionWriter(queues, bufferProvider);
		final BroadcastRecordWriter<SerializationTestType> writer = new BroadcastRecordWriter<>(partitionWriter, 0, "test");
		final RecordDeserializer<SerializationTestType> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(
			new String[]{ tempFolder.getRoot().getAbsolutePath() });

		// generate the configured number of int values as global record set
		final Iterable<SerializationTestType> records = Util.randomRecords(numberOfRecords, SerializationTestTypeFactory.INT);
		// restore the corresponding record set for every input channel
		final Map<Integer, ArrayDeque<SerializationTestType>> serializedRecords = new HashMap<>();
		for (int i = 0; i < numberOfChannels; i++) {
			serializedRecords.put(i, new ArrayDeque<>());
		}

		// every record in global set would both emit into one random channel and broadcast to all the channels
		int index = 0;
		for (SerializationTestType record : records) {
			int randomChannel = index++ % numberOfChannels;
			writer.randomEmit(record, randomChannel);
			serializedRecords.get(randomChannel).add(record);

			writer.broadcastEmit(record);
			for (int i = 0; i < numberOfChannels; i++) {
				serializedRecords.get(i).add(record);
			}
		}

		final int numberOfCreatedBuffers = bufferProvider.getNumberOfCreatedBuffers();
		// verify the expected number of requested buffers, and it would always request a new buffer while random emitting
		assertEquals(numberOfRecords, numberOfCreatedBuffers);

		for (int i = 0; i < numberOfChannels; i++) {
			// every channel would queue the number of above crated buffers
			assertEquals(numberOfRecords, queues[i].size());

			final int excessRandomRecords = i < numberOfRecords % numberOfChannels ? 1 : 0;
			final int numberOfRandomRecords = numberOfRecords / numberOfChannels + excessRandomRecords;
			final int numberOfTotalRecords = numberOfRecords + numberOfRandomRecords;
			// verify the data correctness in every channel queue
			verifyDeserializationResults(
				queues[i],
				deserializer,
				serializedRecords.get(i),
				numberOfCreatedBuffers,
				numberOfTotalRecords);
		}
	}

	/**
	 * FLINK-17780: Tests that a shared buffer(or memory segment) of a buffer builder is only freed when all consumers
	 * are closed.
	 */
	@Test
	public void testRandomEmitAndBufferRecycling() throws Exception {
		int recordSize = 8;

		final TestPooledBufferProvider bufferProvider = new TestPooledBufferProvider(2, 2 * recordSize);
		final KeepingPartitionWriter partitionWriter = new KeepingPartitionWriter(bufferProvider) {
			@Override
			public int getNumberOfSubpartitions() {
				return 2;
			}
		};
		final BroadcastRecordWriter<SerializationTestType> writer = new BroadcastRecordWriter<>(partitionWriter, 0, "test");

		// force materialization of both buffers for easier availability tests
		List<Buffer> buffers = Arrays.asList(bufferProvider.requestBuffer(), bufferProvider.requestBuffer());
		buffers.forEach(Buffer::recycleBuffer);
		assertEquals(2, bufferProvider.getNumberOfAvailableBuffers());

		// fill first buffer
		writer.randomEmit(new IntType(1), 0);
		writer.broadcastEmit(new IntType(2));
		assertEquals(1, bufferProvider.getNumberOfAvailableBuffers());

		// simulate consumption of first buffer consumer; this should not free buffers
		assertEquals(1, partitionWriter.getAddedBufferConsumers(0).size());
		closeConsumer(partitionWriter, 0, 2 * recordSize);
		assertEquals(1, bufferProvider.getNumberOfAvailableBuffers());

		// use second buffer
		writer.broadcastEmit(new IntType(3));
		assertEquals(0, bufferProvider.getNumberOfAvailableBuffers());

		// fully free first buffer
		assertEquals(2, partitionWriter.getAddedBufferConsumers(1).size());
		closeConsumer(partitionWriter, 1, recordSize);
		assertEquals(1, bufferProvider.getNumberOfAvailableBuffers());
	}

	public void closeConsumer(KeepingPartitionWriter partitionWriter, int subpartitionIndex, int expectedSize) {
		BufferConsumer bufferConsumer = partitionWriter.getAddedBufferConsumers(subpartitionIndex).get(0);
		Buffer buffer = bufferConsumer.build();
		bufferConsumer.close();
		assertEquals(expectedSize, buffer.getSize());
		buffer.recycleBuffer();
	}
}
