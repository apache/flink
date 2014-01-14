/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.serialization.RecordDeserializer.DeserializationResult;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestType;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestTypeFactory;
import eu.stratosphere.runtime.io.serialization.types.Util;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayDeque;

public class SpanningRecordSerializationTest {

	@Test
	public void testIntRecordsSpanningMultipleSegments() {
		final int SEGMENT_SIZE = 1;
		final int NUM_VALUES = 10;

		try {
			test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testIntRecordsWithAlignedBuffers () {
		final int SEGMENT_SIZE = 64;
		final int NUM_VALUES = 64;

		try {
			test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testIntRecordsWithUnalignedBuffers () {
		final int SEGMENT_SIZE = 31;
		final int NUM_VALUES = 248;

		try {
			test(Util.randomRecords(NUM_VALUES, SerializationTestTypeFactory.INT), SEGMENT_SIZE);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	 public void testRandomRecords () {
		final int SEGMENT_SIZE = 127;
		final int NUM_VALUES = 10000;

		try {
			test(Util.randomRecords(NUM_VALUES), SEGMENT_SIZE);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Test encountered an unexpected exception.");
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Iterates over the provided records and tests whether {@link SpanningRecordSerializer} and {@link AdaptiveSpanningRecordDeserializer}
	 * interact as expected.
	 * <p>
	 * Only a single {@link MemorySegment} will be allocated.
	 *
	 * @param records records to test
	 * @param segmentSize size for the {@link MemorySegment}
	 */
	private void test (Util.MockRecords records, int segmentSize) throws Exception {
		final int SERIALIZATION_OVERHEAD = 4; // length encoding

		final RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();
		final RecordDeserializer<SerializationTestType> deserializer = new AdaptiveSpanningRecordDeserializer<SerializationTestType>();

		final Buffer buffer = new Buffer(new MemorySegment(new byte[segmentSize]), segmentSize, null);

		final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<SerializationTestType>();

		// -------------------------------------------------------------------------------------------------------------

		serializer.setNextBuffer(buffer);

		int numBytes = 0;
		int numRecords = 0;
		for (SerializationTestType record : records) {

			serializedRecords.add(record);

			numRecords++;
			numBytes += record.length() + SERIALIZATION_OVERHEAD;

			// serialize record
			if (serializer.addRecord(record).isFullBuffer()) {
				// buffer is full => start deserializing
				deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), segmentSize);

				while (!serializedRecords.isEmpty()) {
					SerializationTestType expected = serializedRecords.poll();
					SerializationTestType actual = expected.getClass().newInstance();

					if (deserializer.getNextRecord(actual).isFullRecord()) {
						Assert.assertEquals(expected, actual);
						numRecords--;
					} else {
						serializedRecords.addFirst(expected);
						break;
					}
				}

				while (serializer.setNextBuffer(buffer).isFullBuffer()) {
					deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), segmentSize);
				}



			}
		}

		// deserialize left over records
		deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), (numBytes % segmentSize));

		serializer.clear();

		while (!serializedRecords.isEmpty()) {
			SerializationTestType expected = serializedRecords.poll();

			SerializationTestType actual = expected.getClass().newInstance();
			DeserializationResult result = deserializer.getNextRecord(actual);

			Assert.assertTrue(result.isFullRecord());
			Assert.assertEquals(expected, actual);
			numRecords--;
		}


		// assert that all records have been serialized and deserialized
		Assert.assertEquals(0, numRecords);
		Assert.assertFalse(serializer.hasData());
		Assert.assertFalse(deserializer.hasUnfinishedData());
	}
}
