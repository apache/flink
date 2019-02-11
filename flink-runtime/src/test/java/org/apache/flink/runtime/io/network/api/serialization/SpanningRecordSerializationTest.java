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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.runtime.io.network.util.DeserializationUtils;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferBuilder;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SpillingAdaptiveSpanningRecordDeserializer}.
 */
public class SpanningRecordSerializationTest extends TestLogger {
	private static final Random RANDOM = new Random(42);

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

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

		List<IOReadableWritable> originalRecords = new ArrayList<>((numValues + 1) / 2);
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

	/**
	 * Non-spanning, deserialization reads one byte too many and succeeds - failure report comes
	 * from an additional check in {@link SpillingAdaptiveSpanningRecordDeserializer}.
	 */
	@Test
	public void testHandleDeserializingTooMuchNonSpanning1() throws Exception {
		testHandleWrongDeserialization(
			DeserializingTooMuch.getValue(),
			32 * 1024);
	}

	/**
	 * Non-spanning, deserialization reads one byte too many and fails.
	 */
	@Test
	public void testHandleDeserializingTooMuchNonSpanning2() throws Exception {
		testHandleWrongDeserialization(
			DeserializingTooMuch.getValue(),
			(serializedLength) -> serializedLength,
			isA(IndexOutOfBoundsException.class));
	}

	/**
	 * Spanning, deserialization reads one byte too many and fails.
	 */
	@Test
	public void testHandleDeserializingTooMuchSpanning1() throws Exception {
		testHandleWrongDeserialization(
			DeserializingTooMuch.getValue(),
			(serializedLength) -> serializedLength - 1,
			isA(EOFException.class));
	}

	/**
	 * Spanning, deserialization reads one byte too many and fails.
	 */
	@Test
	public void testHandleDeserializingTooMuchSpanning2() throws Exception {
		testHandleWrongDeserialization(
			DeserializingTooMuch.getValue(),
			(serializedLength) -> 1,
			isA(EOFException.class));
	}

	/**
	 * Spanning, spilling, deserialization reads one byte too many.
	 */
	@Test
	public void testHandleDeserializingTooMuchSpanningLargeRecord() throws Exception {
		testHandleWrongDeserialization(
			LargeObjectTypeDeserializingTooMuch.getRandom(),
			32 * 1024,
			isA(EOFException.class));
	}

	/**
	 * Non-spanning, deserialization forgets to read one byte - failure report comes from an
	 * additional check in {@link SpillingAdaptiveSpanningRecordDeserializer}.
	 */
	@Test
	public void testHandleDeserializingNotEnoughNonSpanning() throws Exception {
		testHandleWrongDeserialization(
			DeserializingNotEnough.getValue(),
			32 * 1024);
	}

	/**
	 * Spanning, deserialization forgets to read one byte - failure report comes from an additional
	 * check in {@link SpillingAdaptiveSpanningRecordDeserializer}.
	 */
	@Test
	public void testHandleDeserializingNotEnoughSpanning1() throws Exception {
		testHandleWrongDeserialization(
			DeserializingNotEnough.getValue(),
			(serializedLength) -> serializedLength - 1);
	}

	/**
	 * Spanning, serialization length is 17 (including headers), deserialization forgets to read one
	 * byte - failure report comes from an additional check in {@link SpillingAdaptiveSpanningRecordDeserializer}.
	 */
	@Test
	public void testHandleDeserializingNotEnoughSpanning2() throws Exception {
		testHandleWrongDeserialization(
			DeserializingNotEnough.getValue(),
			1);
	}

	/**
	 * Spanning, spilling, deserialization forgets to read one byte - failure report comes from an
	 * additional check in {@link SpillingAdaptiveSpanningRecordDeserializer}.
	 */
	@Test
	public void testHandleDeserializingNotEnoughSpanningLargeRecord() throws Exception {
		testHandleWrongDeserialization(
			LargeObjectTypeDeserializingNotEnough.getRandom(),
			32 * 1024);
	}

	private void testHandleWrongDeserialization(
			WrongDeserializationValue testValue,
			IntFunction<Integer> segmentSizeProvider,
			Matcher<? extends Throwable> expectedCause) throws Exception {
		expectedException.expectCause(expectedCause);
		testHandleWrongDeserialization(testValue, segmentSizeProvider);
	}

	private void testHandleWrongDeserialization(
			WrongDeserializationValue testValue,
			@SuppressWarnings("SameParameterValue") int segmentSize,
			Matcher<? extends Throwable> expectedCause) throws Exception {
		expectedException.expectCause(expectedCause);
		testHandleWrongDeserialization(testValue, segmentSize);
	}

	private void testHandleWrongDeserialization(
			WrongDeserializationValue testValue,
			IntFunction<Integer> segmentSizeProvider) throws Exception {
		int serializedBytes = getSerializedBytes(testValue);
		int segmentSize = segmentSizeProvider.apply(serializedBytes);
		testHandleWrongDeserialization(testValue, segmentSize);
	}

	/**
	 * Executes the de/serialization round trip test with a serializer that consumes more or less
	 * bytes than what it writes and verifies the expected exception is thrown.
	 */
	private void testHandleWrongDeserialization(
			WrongDeserializationValue testValue,
			int segmentSize) throws Exception {
		List<IOReadableWritable> originalRecords = new ArrayList<>(1);
		originalRecords.add(testValue);

		expectedException.expect(IOException.class);
		if (testValue instanceof DeserializingTooMuch) {
			expectedException.expectMessage(" -1 remaining unread byte");
		} else if (testValue instanceof DeserializingNotEnough) {
			expectedException.expectMessage(" 1 remaining unread byte");
		} else {
			fail("Invalid test value: " + testValue);
		}

		testSerializationRoundTrip(originalRecords, segmentSize);
	}

	/**
	 * Retrieves the number of bytes the serialized representation of the given value takes (by
	 * doing the serialization).
	 */
	private int getSerializedBytes(IOReadableWritable testValue) throws IOException {
		SpanningRecordSerializer<IOReadableWritable> serializer = new SpanningRecordSerializer<>();
		serializer.serializeRecord(testValue);
		BufferBuilder bufferBuilder = createBufferBuilder();
		assertTrue("Incorrect test setup: buffer not big enough to contain test value",
			serializer.copyToBufferBuilder(bufferBuilder).isFullRecord());
		int writtenBytes = bufferBuilder.finish();
		bufferBuilder.createBufferConsumer().close();
		return writtenBytes;
	}

	/**
	 * Large object that tries to deserialize an additional byte that it has not written during
	 * serialization.
	 */
	public static class LargeObjectTypeDeserializingTooMuch extends LargeObjectType implements DeserializingTooMuch {

		@SuppressWarnings("WeakerAccess")
		public LargeObjectTypeDeserializingTooMuch() {
		}

		@SuppressWarnings("WeakerAccess")
		public LargeObjectTypeDeserializingTooMuch(int len) {
			super(len);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);
			in.readUnsignedByte(); // not written by write()
		}

		@Override
		public LargeObjectTypeDeserializingTooMuch getRandom(Random random) {
			int length = super.getRandom(random).length();
			return new LargeObjectTypeDeserializingTooMuch(length);
		}

		static LargeObjectTypeDeserializingTooMuch getRandom() {
			return (new LargeObjectTypeDeserializingTooMuch()).getRandom(new Random());
		}
	}

	/**
	 * Large object that serializes more bytes than needed and during deserialization will thus
	 * read one byte too few.
	 */
	public static class LargeObjectTypeDeserializingNotEnough extends LargeObjectType implements DeserializingNotEnough {

		@SuppressWarnings("WeakerAccess")
		public LargeObjectTypeDeserializingNotEnough() {
		}

		@SuppressWarnings("WeakerAccess")
		public LargeObjectTypeDeserializingNotEnough(int len) {
			super(len);
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			out.write(42); // not used in read()
		}

		@Override
		public LargeObjectTypeDeserializingNotEnough getRandom(Random random) {
			int length = super.getRandom(random).length();
			return new LargeObjectTypeDeserializingNotEnough(length);
		}

		static LargeObjectTypeDeserializingNotEnough getRandom() {
			return (new LargeObjectTypeDeserializingNotEnough()).getRandom(new Random());
		}
	}

	// -----------------------------------------------------------------------------------------------------------------

	private void testSerializationRoundTrip(Iterable<? extends IOReadableWritable> records, int segmentSize) throws Exception {
		RecordSerializer<IOReadableWritable> serializer = new SpanningRecordSerializer<>();
		RecordDeserializer<IOReadableWritable> deserializer =
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
			Iterable<? extends IOReadableWritable> records,
			int segmentSize,
			RecordSerializer<IOReadableWritable> serializer,
			RecordDeserializer<IOReadableWritable> deserializer)
		throws Exception {
		final ArrayDeque<IOReadableWritable> serializedRecords = new ArrayDeque<>();

		// -------------------------------------------------------------------------------------------------------------

		BufferAndSerializerResult serializationResult = setNextBufferForSerializer(serializer, segmentSize);

		int numRecords = 0;
		for (IOReadableWritable record : records) {

			serializedRecords.add(record);

			numRecords++;

			// serialize record
			serializer.serializeRecord(record);
			if (serializer.copyToBufferBuilder(serializationResult.getBufferBuilder()).isFullBuffer()) {
				// buffer is full => start deserializing
				deserializer.setNextBuffer(serializationResult.buildBuffer());

				numRecords -= DeserializationUtils.deserializeRecords(serializedRecords, deserializer, false);

				// move buffers as long as necessary (for spanning records)
				while ((serializationResult = setNextBufferForSerializer(serializer, segmentSize)).isFullBuffer()) {
					deserializer.setNextBuffer(serializationResult.buildBuffer());
					numRecords -= DeserializationUtils.deserializeRecords(serializedRecords, deserializer, false);
				}
			}
		}

		// deserialize left over records
		deserializer.setNextBuffer(serializationResult.buildBuffer());

		numRecords -= DeserializationUtils.deserializeRecords(serializedRecords, deserializer, true);

		// assert that all records have been serialized and deserialized
		Assert.assertEquals(0, numRecords);
		Assert.assertFalse(serializer.hasSerializedData());
		Assert.assertFalse(deserializer.hasUnfinishedData());
	}

	private static BufferAndSerializerResult setNextBufferForSerializer(
			RecordSerializer<IOReadableWritable> serializer,
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

	private interface WrongDeserializationValue extends IOReadableWritable {
	}

	private interface DeserializingTooMuch extends WrongDeserializationValue {
		/**
		 * Gets a randomly created value with a serializer that deserializes more bytes than it writes.
		 */
		static DeserializingTooMuch getValue() {
			return new LargeObjectTypeDeserializingTooMuch(
				ThreadLocalRandom.current().nextInt(0, 1024));
		}
	}

	private interface DeserializingNotEnough extends WrongDeserializationValue {
		/**
		 * Gets a randomly created value with a serializer that deserializes less bytes than it writes.
		 */
		static DeserializingNotEnough getValue() {
			return new LargeObjectTypeDeserializingNotEnough(
				ThreadLocalRandom.current().nextInt(0, 1024));
		}
	}
}
