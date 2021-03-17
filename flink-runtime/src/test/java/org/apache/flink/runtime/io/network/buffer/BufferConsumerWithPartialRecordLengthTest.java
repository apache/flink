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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;

import org.junit.After;
import org.junit.Test;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest.assertContent;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderAndConsumerTest.toByteBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BufferConsumerWithPartialRecordLength}. */
public class BufferConsumerWithPartialRecordLengthTest {
    private static final int BUFFER_INT_SIZE = 4;
    private static final int BUFFER_SIZE = BUFFER_INT_SIZE * Integer.BYTES;
    private final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =
            new PrioritizedDeque<>();

    private BufferBuilder builder = null;

    @After
    public void clear() {
        buffers.clear();
        builder = null;
    }

    @Test
    public void partialRecordTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 42));

        assertEquals(buffers.size(), 2);

        // buffer starts with a full record
        BufferConsumerWithPartialRecordLength consumer1 = buffers.poll();
        assertEquals(0, requireNonNull(consumer1).getPartialRecordLength());
        assertTrue(consumer1.cleanupPartialRecord());
        assertContent(consumer1.build(), FreeingBufferRecycler.INSTANCE, 0, 1, 2, 3);

        // buffer starts with partial record, partial record ends within the buffer
        // skip the partial record, return an empty buffer
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertTrue(requireNonNull(consumer2).cleanupPartialRecord());
        assertEquals(consumer2.build().readableBytes(), 0);
    }

    @Test
    public void partialLongRecordSpanningBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 42));
        writeToBuffer(toByteBuffer(8, 9));

        assertEquals(buffers.size(), 3);
        buffers.poll();

        // long partial record spanning over the entire buffer, clean up not successful
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertEquals(BUFFER_SIZE, requireNonNull(consumer2).getPartialRecordLength());
        assertFalse(consumer2.cleanupPartialRecord());
        assertEquals(consumer2.build().readableBytes(), 0);

        BufferConsumerWithPartialRecordLength consumer3 = buffers.poll();
        assertTrue(requireNonNull(consumer3).cleanupPartialRecord());
        assertContent(consumer3.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    @Test
    public void partialLongRecordEndsWithFullBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 42));
        writeToBuffer(toByteBuffer(8, 9));

        assertEquals(buffers.size(), 3);
        buffers.poll();

        // long partial record ends at the end of the buffer, clean up not successful
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertEquals(BUFFER_SIZE, requireNonNull(consumer2).getPartialRecordLength());
        assertFalse(consumer2.cleanupPartialRecord());
        assertEquals(consumer2.build().readableBytes(), 0);

        BufferConsumerWithPartialRecordLength consumer3 = buffers.poll();
        assertTrue(requireNonNull(consumer3).cleanupPartialRecord());
        assertContent(consumer3.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    @Test
    public void readPositionNotAtTheBeginningOfTheBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 42));

        assertEquals(buffers.size(), 2);
        buffers.poll();

        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        requireNonNull(consumer2).build();

        // read not start from the beginning of the buffer
        writeToBuffer(toByteBuffer(8, 9));
        assertEquals(4, consumer2.getPartialRecordLength());
        assertTrue(consumer2.cleanupPartialRecord());
        assertContent(consumer2.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    private void writeToBuffer(ByteBuffer record) {
        if (builder == null) {
            builder = createBufferBuilder();
            buffers.add(
                    new BufferConsumerWithPartialRecordLength(
                            builder.createBufferConsumerFromBeginning(), 0));
        }
        builder.appendAndCommit(record);

        while (record.hasRemaining()) {
            builder.finish();
            builder = createBufferBuilder();
            final int partialRecordBytes = builder.appendAndCommit(record);
            buffers.add(
                    new BufferConsumerWithPartialRecordLength(
                            builder.createBufferConsumerFromBeginning(), partialRecordBytes));
        }

        if (builder.isFull()) {
            builder.finish();
            builder = null;
        }
    }

    private BufferBuilder createBufferBuilder() {
        return new BufferBuilder(
                MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE),
                FreeingBufferRecycler.INSTANCE);
    }
}
