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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.runtime.io.network.util.DeserializationUtils;
import org.apache.flink.testutils.serialization.types.IntType;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferBuilder;

/** Tests for the {@link SpillingAdaptiveSpanningRecordDeserializer}. */
public class SpanningRecordSerializationTest extends TestLogger {
    private static final Random RANDOM = new Random(42);

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testIntRecordsSpanningMultipleSegments() throws Exception {
        final int segmentSize = 1;
        final int numValues = 10;

        testSerializationRoundTrip(
                Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
    }

    @Test
    public void testIntRecordsWithAlignedBuffers() throws Exception {
        final int segmentSize = 64;
        final int numValues = 64;

        testSerializationRoundTrip(
                Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
    }

    @Test
    public void testIntRecordsWithUnalignedBuffers() throws Exception {
        final int segmentSize = 31;
        final int numValues = 248;

        testSerializationRoundTrip(
                Util.randomRecords(numValues, SerializationTestTypeFactory.INT), segmentSize);
    }

    @Test
    public void testRandomRecords() throws Exception {
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

    private void testSerializationRoundTrip(
            Iterable<SerializationTestType> records, int segmentSize) throws Exception {
        RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempFolder.getRoot().getAbsolutePath()});

        testSerializationRoundTrip(records, segmentSize, deserializer);
    }

    /**
     * Iterates over the provided records and tests whether {@link RecordWriter#serializeRecord} and
     * {@link RecordDeserializer} interact as expected.
     *
     * <p>Only a single {@link MemorySegment} will be allocated.
     *
     * @param records records to test
     * @param segmentSize size for the {@link MemorySegment}
     */
    private static void testSerializationRoundTrip(
            Iterable<SerializationTestType> records,
            int segmentSize,
            RecordDeserializer<SerializationTestType> deserializer)
            throws Exception {
        final DataOutputSerializer serializer = new DataOutputSerializer(128);
        final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<>();

        // -------------------------------------------------------------------------------------------------------------

        BufferAndSerializerResult serializationResult =
                setNextBufferForSerializer(serializer.wrapAsByteBuffer(), segmentSize);

        int numRecords = 0;
        for (SerializationTestType record : records) {

            serializedRecords.add(record);

            numRecords++;

            // serialize record
            serializer.clear();
            ByteBuffer serializedRecord = RecordWriter.serializeRecord(serializer, record);
            serializationResult.getBufferBuilder().appendAndCommit(serializedRecord);
            if (serializationResult.getBufferBuilder().isFull()) {
                // buffer is full => start deserializing
                deserializer.setNextBuffer(serializationResult.buildBuffer());

                numRecords -=
                        DeserializationUtils.deserializeRecords(serializedRecords, deserializer);

                // move buffers as long as necessary (for long records)
                while ((serializationResult =
                                setNextBufferForSerializer(serializedRecord, segmentSize))
                        .isFullBuffer()) {
                    deserializer.setNextBuffer(serializationResult.buildBuffer());
                }
            }
            Assert.assertFalse(serializedRecord.hasRemaining());
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
        Assert.assertFalse(deserializer.hasUnfinishedData());
    }

    @Test
    public void testSmallRecordUnconsumedBuffer() throws Exception {
        RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempFolder.getRoot().getAbsolutePath()});

        testUnconsumedBuffer(
                deserializer, Util.randomRecord(SerializationTestTypeFactory.INT), 1024);
    }

    /**
     * Test both for spanning records and for handling the length buffer, that's why it's going byte
     * by byte.
     */
    @Test
    public void testSpanningRecordUnconsumedBuffer() throws Exception {
        RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempFolder.getRoot().getAbsolutePath()});

        testUnconsumedBuffer(deserializer, Util.randomRecord(SerializationTestTypeFactory.INT), 1);
    }

    @Test
    public void testLargeSpanningRecordUnconsumedBuffer() throws Exception {
        RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempFolder.getRoot().getAbsolutePath()});

        testUnconsumedBuffer(
                deserializer, Util.randomRecord(SerializationTestTypeFactory.BYTE_ARRAY), 1);
    }

    @Test
    public void testLargeSpanningRecordUnconsumedBufferWithLeftOverBytes() throws Exception {
        RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempFolder.getRoot().getAbsolutePath()});

        testUnconsumedBuffer(
                deserializer,
                Util.randomRecord(SerializationTestTypeFactory.BYTE_ARRAY),
                1,
                new byte[] {42, 43, 44});

        deserializer.clear();

        testUnconsumedBuffer(
                deserializer,
                Util.randomRecord(SerializationTestTypeFactory.BYTE_ARRAY),
                1,
                new byte[] {42, 43, 44});
    }

    public void testUnconsumedBuffer(
            RecordDeserializer<SerializationTestType> deserializer,
            SerializationTestType record,
            int segmentSize,
            byte... leftOverBytes)
            throws Exception {
        try (ByteArrayOutputStream unconsumedBytes = new ByteArrayOutputStream()) {
            DataOutputSerializer serializer = new DataOutputSerializer(128);
            ByteBuffer serializedRecord = RecordWriter.serializeRecord(serializer, record);

            BufferAndSerializerResult serializationResult =
                    setNextBufferForSerializer(serializedRecord, segmentSize);

            serializationResult.getBufferBuilder().appendAndCommit(serializedRecord);
            if (serializationResult.getBufferBuilder().isFull()) {
                // buffer is full => start deserializing
                Buffer buffer = serializationResult.buildBuffer();
                writeBuffer(buffer.readOnlySlice().getNioBufferReadable(), unconsumedBytes);
                deserializer.setNextBuffer(buffer);
                assertUnconsumedBuffer(unconsumedBytes, deserializer.getUnconsumedBuffer());

                deserializer.getNextRecord(record.getClass().newInstance());

                // move buffers as long as necessary (for long records)
                while ((serializationResult =
                                setNextBufferForSerializer(serializedRecord, segmentSize))
                        .isFullBuffer()) {
                    buffer = serializationResult.buildBuffer();

                    if (serializationResult.isFullRecord()) {
                        buffer = appendLeftOverBytes(buffer, leftOverBytes);
                    }

                    writeBuffer(buffer.readOnlySlice().getNioBufferReadable(), unconsumedBytes);
                    deserializer.setNextBuffer(buffer);
                    assertUnconsumedBuffer(unconsumedBytes, deserializer.getUnconsumedBuffer());

                    deserializer.getNextRecord(record.getClass().newInstance());
                }
            }
        }
    }

    private static Buffer appendLeftOverBytes(Buffer buffer, byte[] leftOverBytes) {
        BufferBuilder bufferBuilder =
                new BufferBuilder(
                        MemorySegmentFactory.allocateUnpooledSegment(
                                buffer.readableBytes() + leftOverBytes.length),
                        FreeingBufferRecycler.INSTANCE);
        try (BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer()) {
            bufferBuilder.append(buffer.getNioBufferReadable());
            bufferBuilder.appendAndCommit(ByteBuffer.wrap(leftOverBytes));
            return bufferConsumer.build();
        }
    }

    private static void assertUnconsumedBuffer(
            ByteArrayOutputStream expected, CloseableIterator<Buffer> actual) throws Exception {
        if (!actual.hasNext()) {
            Assert.assertEquals(expected.size(), 0);
        }

        ByteBuffer expectedByteBuffer = ByteBuffer.wrap(expected.toByteArray());
        ByteBuffer actualByteBuffer = actual.next().getNioBufferReadable();
        Assert.assertEquals(expectedByteBuffer, actualByteBuffer);
        actual.close();
    }

    private static void writeBuffer(ByteBuffer buffer, OutputStream stream) throws IOException {
        WritableByteChannel channel = Channels.newChannel(stream);
        channel.write(buffer);
    }

    private static BufferAndSerializerResult setNextBufferForSerializer(
            ByteBuffer serializedRecord, int segmentSize) throws IOException {
        // create a bufferBuilder with some random starting offset to properly test handling buffer
        // slices in the
        // deserialization code.
        int startingOffset = segmentSize > 2 ? RANDOM.nextInt(segmentSize / 2) : 0;
        BufferBuilder bufferBuilder =
                createFilledBufferBuilder(segmentSize + startingOffset, startingOffset);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
        bufferConsumer.build().recycleBuffer();

        bufferBuilder.appendAndCommit(serializedRecord);
        return new BufferAndSerializerResult(
                bufferBuilder,
                bufferConsumer,
                bufferBuilder.isFull(),
                !serializedRecord.hasRemaining());
    }

    private static class BufferAndSerializerResult {
        private final BufferBuilder bufferBuilder;
        private final BufferConsumer bufferConsumer;
        private final boolean isFullBuffer;
        private final boolean isFullRecord;

        public BufferAndSerializerResult(
                BufferBuilder bufferBuilder,
                BufferConsumer bufferConsumer,
                boolean isFullBuffer,
                boolean isFullRecord) {
            this.bufferBuilder = bufferBuilder;
            this.bufferConsumer = bufferConsumer;
            this.isFullBuffer = isFullBuffer;
            this.isFullRecord = isFullRecord;
        }

        public BufferBuilder getBufferBuilder() {
            return bufferBuilder;
        }

        public Buffer buildBuffer() {
            return buildSingleBuffer(bufferConsumer);
        }

        public boolean isFullBuffer() {
            return isFullBuffer;
        }

        public boolean isFullRecord() {
            return isFullRecord;
        }
    }
}
