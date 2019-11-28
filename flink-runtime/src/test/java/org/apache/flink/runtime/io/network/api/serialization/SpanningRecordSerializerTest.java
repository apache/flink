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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;

/**
 * Tests for the {@link SpanningRecordSerializer}.
 */
public class SpanningRecordSerializerTest {

	@Test
	public void testHasSerializedData() throws IOException {
		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();
		final SerializationTestType randomIntRecord = Util.randomRecord(SerializationTestTypeFactory.INT);

		Assert.assertFalse(serializer.hasSerializedData());

		serializer.serializeRecord(randomIntRecord);
		Assert.assertTrue(serializer.hasSerializedData());

		final BufferBuilder bufferBuilder1 = createBufferBuilder(16);
		serializer.copyToBufferBuilder(bufferBuilder1);
		Assert.assertFalse(serializer.hasSerializedData());

		final BufferBuilder bufferBuilder2 = createBufferBuilder(8);
		serializer.reset();
		serializer.copyToBufferBuilder(bufferBuilder2);
		Assert.assertFalse(serializer.hasSerializedData());

		serializer.reset();
		serializer.copyToBufferBuilder(bufferBuilder2);
		// Buffer builder full!
		Assert.assertTrue(serializer.hasSerializedData());
	}

	@Test
	public void testEmptyRecords() throws IOException {
		final int segmentSize = 11;

		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();
		final BufferBuilder bufferBuilder1 = createBufferBuilder(segmentSize);

		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD,
			serializer.copyToBufferBuilder(bufferBuilder1));

		SerializationTestType emptyRecord = new SerializationTestType() {
			@Override
			public SerializationTestType getRandom(Random rnd) {
				throw new UnsupportedOperationException();
			}

			@Override
			public int length() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void write(DataOutputView out) {}

			@Override
			public void read(DataInputView in) {}

			@Override
			public int hashCode() {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean equals(Object obj) {
				throw new UnsupportedOperationException();
			}
		};

		serializer.serializeRecord(emptyRecord);
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, serializer.copyToBufferBuilder(bufferBuilder1));

		serializer.reset();
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, serializer.copyToBufferBuilder(bufferBuilder1));

		serializer.reset();
		Assert.assertEquals(RecordSerializer.SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL,
			serializer.copyToBufferBuilder(bufferBuilder1));

		final BufferBuilder bufferBuilder2 = createBufferBuilder(segmentSize);
		Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD,
			serializer.copyToBufferBuilder(bufferBuilder2));
	}

	@Test
	public void testIntRecordsSpanningMultipleSegments() throws Exception {
		final int segmentSize = 1;
		final int numValues = 10;

		test(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithAlignedSegments() throws Exception {
		final int segmentSize = 64;
		final int numValues = 64;

		test(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testIntRecordsWithUnalignedSegments() throws Exception {
		final int segmentSize = 31;
		final int numValues = 248; // least common multiple => last record should align

		test(Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
	}

	@Test
	public void testRandomRecords() throws Exception {
		final int segmentSize = 127;
		final int numValues = 100000;

		test(Util.randomRecords(numValues), segmentSize);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Iterates over the provided records and tests whether the {@link SpanningRecordSerializer} returns the expected
	 * {@link RecordSerializer.SerializationResult} values.
	 *
	 * <p>Only a single {@link MemorySegment} will be allocated.
	 *
	 * @param records records to test
	 * @param segmentSize size for the {@link MemorySegment}
	 */
	private void test(Util.MockRecords records, int segmentSize) throws Exception {
		final int serializationOverhead = 4; // length encoding

		final SpanningRecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<>();

		// -------------------------------------------------------------------------------------------------------------

		BufferBuilder bufferBuilder = createBufferBuilder(segmentSize);
		int numBytes = 0;
		for (SerializationTestType record : records) {
			serializer.serializeRecord(record);
			RecordSerializer.SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
			numBytes += record.length() + serializationOverhead;

			if (numBytes < segmentSize) {
				Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD, result);
			} else if (numBytes == segmentSize) {
				Assert.assertEquals(RecordSerializer.SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL, result);
				bufferBuilder = createBufferBuilder(segmentSize);
				numBytes = 0;
			} else {
				Assert.assertEquals(RecordSerializer.SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL, result);

				while (result.isFullBuffer()) {
					numBytes -= segmentSize;
					bufferBuilder = createBufferBuilder(segmentSize);
					result = serializer.copyToBufferBuilder(bufferBuilder);
				}

				Assert.assertTrue(result.isFullRecord());
			}
		}
	}
}
