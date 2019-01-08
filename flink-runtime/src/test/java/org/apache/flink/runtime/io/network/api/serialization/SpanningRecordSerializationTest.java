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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.runtime.io.network.util.DeserializationUtils;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferBuilder;

/**
 * Tests for the {@link SpillingAdaptiveSpanningRecordDeserializer}.
 */
public class SpanningRecordSerializationTest extends TestLogger {
	private static final Random RANDOM = new Random(42);

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testIntRecordsSpanningMultipleSegments() throws Exception {
		final int segmentSize = 1;
		final int numValues = 10;

		testSerializationRoundTrip(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithAlignedBuffers () throws Exception {
		final int segmentSize = 64;
		final int numValues = 64;

		testSerializationRoundTrip(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithUnalignedBuffers () throws Exception {
		final int segmentSize = 31;
		final int numValues = 248;

		testSerializationRoundTrip(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testRandomRecords () throws Exception {
		final int segmentSize = 127;
		final int numValues = 10000;

		testSerializationRoundTrip(Util.randomRecords(numValues), segmentSize);
	}

	@Test
	public void testHandleMixedLargeRecords() throws Exception {
		final int numValues = 99;
		final int segmentSize = 32 * 1024;

		List<SerializationTestType> originalRecords = new ArrayList<>((numValues + 1) / 2);
		LargeObjectType genLarge = new LargeObjectType();
		Random rnd = new Random();

		for (int i = 0; i < numValues; i++) {
			if (i % 2 == 0) {
				originalRecords.add(new IntType(42));
			} else {
				originalRecords.add(genLarge.getRandom(rnd));
			}
		}

		testSerializationRoundTrip(originalRecords, segmentSize);
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void testSerializationRoundTrip(Iterable<SerializationTestType> records, int segmentSize) throws Exception {
		RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();
		RecordDeserializer<SerializationTestType> deserializer =
			new SpillingAdaptiveSpanningRecordDeserializer<>(
				new String[]{ tempFolder.getRoot().getAbsolutePath() });

		testSerializationRoundTrip(records, segmentSize, serializer, deserializer);
	}

	/**
	 * Iterates over the provided records and tests whether {@link SpanningRecordSerializer} and {@link RecordDeserializer}
	 * interact as expected.
	 *
	 * <p>Only a single {@link MemorySegment} will be allocated.
	 *
	 * @param records records to test
	 * @param segmentSize size for the {@link MemorySegment}
	 */
	private static void testSerializationRoundTrip(
			Iterable<SerializationTestType> records,
			int segmentSize,
			RecordSerializer<SerializationTestType> serializer,
			RecordDeserializer<SerializationTestType> deserializer)
		throws Exception {
		final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<>();

		// -------------------------------------------------------------------------------------------------------------

		BufferAndSerializerResult serializationResult = setNextBufferForSerializer(serializer, segmentSize);

		int numRecords = 0;
		for (SerializationTestType record : records) {

			serializedRecords.add(record);

			numRecords++;

			// serialize record
			serializer.serializeRecord(record);
			if (serializer.copyToBufferBuilder(serializationResult.getBufferBuilder()).isFullBuffer()) {
				// buffer is full => start deserializing
				deserializer.setNextBuffer(serializationResult.buildBuffer());

				numRecords -= DeserializationUtils.deserializeRecords(serializedRecords, deserializer);

				// move buffers as long as necessary (for long records)
				while ((serializationResult = setNextBufferForSerializer(serializer, segmentSize)).isFullBuffer()) {
					deserializer.setNextBuffer(serializationResult.buildBuffer());
				}
			}
		}

		// deserialize left over records
		deserializer.setNextBuffer(serializationResult.buildBuffer());

		while (!serializedRecords.isEmpty()) {
			SerializationTestType expected = serializedRecords.poll();

			SerializationTestType actual = expected.getClass().newInstance();
			RecordDeserializer.DeserializationResult result = deserializer.getNextRecord(actual);

			Assert.assertTrue(result.isFullRecord());
			Assert.assertEquals(expected, actual);
			numRecords--;
		}

		// assert that all records have been serialized and deserialized
		Assert.assertEquals(0, numRecords);
		Assert.assertFalse(serializer.hasSerializedData());
		Assert.assertFalse(deserializer.hasUnfinishedData());
	}

	private static BufferAndSerializerResult setNextBufferForSerializer(
			RecordSerializer<SerializationTestType> serializer,
			int segmentSize) throws IOException {
		// create a bufferBuilder with some random starting offset to properly test handling buffer slices in the
		// deserialization code.
		int startingOffset = segmentSize > 2 ? RANDOM.nextInt(segmentSize / 2) : 0;
		BufferBuilder bufferBuilder = createFilledBufferBuilder(segmentSize + startingOffset, startingOffset);
		BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
		bufferConsumer.build().recycleBuffer();

		return new BufferAndSerializerResult(
			bufferBuilder,
			bufferConsumer,
			serializer.copyToBufferBuilder(bufferBuilder));
	}

	private static class BufferAndSerializerResult {
		private final BufferBuilder bufferBuilder;
		private final BufferConsumer bufferConsumer;
		private final RecordSerializer.SerializationResult serializationResult;

		public BufferAndSerializerResult(
				BufferBuilder bufferBuilder,
				BufferConsumer bufferConsumer,
				RecordSerializer.SerializationResult serializationResult) {
			this.bufferBuilder = bufferBuilder;
			this.bufferConsumer = bufferConsumer;
			this.serializationResult = serializationResult;
		}

		public BufferBuilder getBufferBuilder() {
			return bufferBuilder;
		}

		public Buffer buildBuffer() {
			return buildSingleBuffer(bufferConsumer);
		}

		public boolean isFullBuffer() {
			return serializationResult.isFullBuffer();
		}
	}
}
