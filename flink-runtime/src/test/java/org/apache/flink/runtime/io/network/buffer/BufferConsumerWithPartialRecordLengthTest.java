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

import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.assertContent;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createEmptyBufferBuilder;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.toByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BufferConsumerWithPartialRecordLength}. */
class BufferConsumerWithPartialRecordLengthTest {
    private static final int BUFFER_INT_SIZE = 4;
    private static final int BUFFER_SIZE = BUFFER_INT_SIZE * Integer.BYTES;
    private final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =
            new PrioritizedDeque<>();

    private BufferBuilder builder = null;

    @AfterEach
    void clear() {
        buffers.clear();
        builder = null;
    }

    @Test
    void partialRecordTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 42));

        assertThat(buffers).hasSize(2);

        // buffer starts with a full record
        BufferConsumerWithPartialRecordLength consumer1 = buffers.poll();
        assertThat(requireNonNull(consumer1).getPartialRecordLength()).isZero();
        assertThat(consumer1.cleanupPartialRecord()).isTrue();
        assertContent(consumer1.build(), FreeingBufferRecycler.INSTANCE, 0, 1, 2, 3);

        // buffer starts with partial record, partial record ends within the buffer
        // skip the partial record, return an empty buffer
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertThat(requireNonNull(consumer2).cleanupPartialRecord()).isTrue();
        assertThat(consumer2.build().readableBytes()).isZero();
    }

    @Test
    void partialLongRecordSpanningBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 42));
        writeToBuffer(toByteBuffer(8, 9));

        assertThat(buffers).hasSize(3);
        buffers.poll();

        // long partial record spanning over the entire buffer, clean up not successful
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertThat(requireNonNull(consumer2).getPartialRecordLength()).isEqualTo(BUFFER_SIZE);
        assertThat(consumer2.cleanupPartialRecord()).isFalse();
        assertThat(consumer2.build().readableBytes()).isZero();

        BufferConsumerWithPartialRecordLength consumer3 = buffers.poll();
        assertThat(requireNonNull(consumer3).cleanupPartialRecord()).isTrue();
        assertContent(consumer3.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    @Test
    void partialLongRecordEndsWithFullBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 4, 5, 6, 42));
        writeToBuffer(toByteBuffer(8, 9));

        assertThat(buffers).hasSize(3);
        buffers.poll();

        // long partial record ends at the end of the buffer, clean up not successful
        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        assertThat(requireNonNull(consumer2).getPartialRecordLength()).isEqualTo(BUFFER_SIZE);
        assertThat(consumer2.cleanupPartialRecord()).isFalse();
        assertThat(consumer2.build().readableBytes()).isZero();

        BufferConsumerWithPartialRecordLength consumer3 = buffers.poll();
        assertThat(requireNonNull(consumer3).cleanupPartialRecord()).isTrue();
        assertContent(consumer3.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    @Test
    void readPositionNotAtTheBeginningOfTheBufferTestCase() {
        writeToBuffer(toByteBuffer(0, 1, 2, 3, 42));

        assertThat(buffers).hasSize(2);
        buffers.poll();

        BufferConsumerWithPartialRecordLength consumer2 = buffers.poll();
        requireNonNull(consumer2).build();

        // read not start from the beginning of the buffer
        writeToBuffer(toByteBuffer(8, 9));
        assertThat(consumer2.getPartialRecordLength()).isEqualTo(4);
        assertThat(consumer2.cleanupPartialRecord()).isTrue();
        assertContent(consumer2.build(), FreeingBufferRecycler.INSTANCE, 8, 9);
    }

    private void writeToBuffer(ByteBuffer record) {
        if (builder == null) {
            builder = createEmptyBufferBuilder(BUFFER_SIZE);
            buffers.add(
                    new BufferConsumerWithPartialRecordLength(
                            builder.createBufferConsumerFromBeginning(), 0));
        }
        builder.appendAndCommit(record);

        while (record.hasRemaining()) {
            builder.finish();
            builder = createEmptyBufferBuilder(BUFFER_SIZE);
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
}
