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

import org.apache.flink.core.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferBuilder}. */
public class BufferBuilderAndConsumerTest {
    private static final int BUFFER_INT_SIZE = 10;
    private static final int BUFFER_SIZE = BUFFER_INT_SIZE * Integer.BYTES;

    @Test
    void referenceCounting() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        assertThat(bufferBuilder.appendAndCommit(toByteBuffer(1, 2, 3)))
                .isEqualTo(3 * Integer.BYTES);

        bufferBuilder.close();

        Buffer buffer = bufferConsumer.build();
        assertThat(buffer.isRecycled()).isFalse();
        buffer.recycleBuffer();
        assertThat(buffer.isRecycled()).isFalse();
        bufferConsumer.close();
        assertThat(buffer.isRecycled()).isTrue();
    }

    @Test
    void append() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        int[] intsToWrite = new int[] {0, 1, 2, 3, 42};
        ByteBuffer bytesToWrite = toByteBuffer(intsToWrite);

        assertThat(bufferBuilder.appendAndCommit(bytesToWrite)).isEqualTo(bytesToWrite.limit());

        assertThat(bytesToWrite.position()).isEqualTo(bytesToWrite.limit());
        assertThat(bufferBuilder.isFull()).isFalse();

        assertContent(bufferConsumer, intsToWrite);
    }

    @Test
    void multipleAppends() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        bufferBuilder.appendAndCommit(toByteBuffer(0, 1));
        bufferBuilder.appendAndCommit(toByteBuffer(2));
        bufferBuilder.appendAndCommit(toByteBuffer(3, 42));

        assertContent(bufferConsumer, 0, 1, 2, 3, 42);
    }

    @Test
    void multipleNotCommittedAppends() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        bufferBuilder.append(toByteBuffer(0, 1));
        bufferBuilder.append(toByteBuffer(2));
        bufferBuilder.append(toByteBuffer(3, 42));

        assertContent(bufferConsumer);

        bufferBuilder.commit();

        assertContent(bufferConsumer, 0, 1, 2, 3, 42);
    }

    @Test
    void appendOverSize() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
        ByteBuffer bytesToWrite = toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42);

        assertThat(bufferBuilder.appendAndCommit(bytesToWrite)).isEqualTo(BUFFER_SIZE);

        assertThat(bufferBuilder.isFull()).isTrue();
        assertContent(bufferConsumer, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bufferBuilder = createBufferBuilder();
        bufferConsumer = bufferBuilder.createBufferConsumer();
        assertThat(bufferBuilder.appendAndCommit(bytesToWrite)).isEqualTo(Integer.BYTES);

        assertThat(bufferBuilder.isFull()).isFalse();
        assertContent(bufferConsumer, 42);
    }

    @Test
    void creatingBufferConsumerTwice() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        bufferBuilder.createBufferConsumer();
        assertThatThrownBy(bufferBuilder::createBufferConsumer)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void copy() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer1 = bufferBuilder.createBufferConsumer();

        bufferBuilder.appendAndCommit(toByteBuffer(0, 1));

        BufferConsumer bufferConsumer2 = bufferConsumer1.copy();

        bufferBuilder.appendAndCommit(toByteBuffer(2));

        assertContent(bufferConsumer1, 0, 1, 2);
        assertContent(bufferConsumer2, 0, 1, 2);

        BufferConsumer bufferConsumer3 = bufferConsumer1.copy();

        bufferBuilder.appendAndCommit(toByteBuffer(3, 42));

        BufferConsumer bufferConsumer4 = bufferConsumer1.copy();

        assertContent(bufferConsumer1, 3, 42);
        assertContent(bufferConsumer2, 3, 42);
        assertContent(bufferConsumer3, 3, 42);
        assertContent(bufferConsumer4, 3, 42);
    }

    @Test
    void buildEmptyBuffer() {
        try (BufferBuilder bufferBuilder = createBufferBuilder()) {
            Buffer buffer = buildSingleBuffer(bufferBuilder);
            assertThat(buffer.getSize()).isZero();
            assertContent(buffer, FreeingBufferRecycler.INSTANCE);
        }
    }

    @Test
    void buildingBufferMultipleTimes() {
        try (BufferBuilder bufferBuilder = createBufferBuilder()) {
            try (BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer()) {
                bufferBuilder.appendAndCommit(toByteBuffer(0, 1));
                bufferBuilder.appendAndCommit(toByteBuffer(2));

                assertContent(bufferConsumer, 0, 1, 2);

                bufferBuilder.appendAndCommit(toByteBuffer(3, 42));
                bufferBuilder.appendAndCommit(toByteBuffer(44));

                assertContent(bufferConsumer, 3, 42, 44);

                ArrayList<Integer> originalValues = new ArrayList<>();
                while (!bufferBuilder.isFull()) {
                    bufferBuilder.appendAndCommit(toByteBuffer(1337));
                    originalValues.add(1337);
                }

                assertContent(
                        bufferConsumer,
                        originalValues.stream().mapToInt(Integer::intValue).toArray());
            }
        }
    }

    @Test
    void emptyIsFinished() {
        testIsFinished(0);
    }

    @Test
    void partiallyFullIsFinished() {
        testIsFinished(BUFFER_INT_SIZE / 2);
    }

    @Test
    void fullIsFinished() {
        testIsFinished(BUFFER_INT_SIZE);
    }

    @Test
    void testWritableBytes() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertThat(bufferBuilder.getWritableBytes()).isEqualTo(bufferBuilder.getMaxCapacity());

        ByteBuffer byteBuffer = toByteBuffer(1, 2, 3);
        bufferBuilder.append(byteBuffer);
        assertThat(bufferBuilder.getWritableBytes())
                .isEqualTo(bufferBuilder.getMaxCapacity() - byteBuffer.position());

        assertThat(bufferBuilder.getWritableBytes())
                .isEqualTo(bufferBuilder.getMaxCapacity() - byteBuffer.position());
    }

    @Test
    void testWritableBytesWhenFull() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        bufferBuilder.append(toByteBuffer(new int[bufferBuilder.getMaxCapacity()]));
        assertThat(bufferBuilder.getWritableBytes()).isZero();
    }

    @Test
    void recycleWithoutConsumer() {
        // given: Recycler with the counter of recycle invocation.
        CountedRecycler recycler = new CountedRecycler();
        BufferBuilder bufferBuilder =
                new BufferBuilder(allocateUnpooledSegment(BUFFER_SIZE), recycler);

        // when: Invoke the recycle.
        bufferBuilder.close();

        // then: Recycling successfully finished.
        assertThat(recycler.recycleInvocationCounter).isOne();
    }

    @Test
    void recycleConsumerAndBufferBuilder() {
        // given: Recycler with the counter of recycling invocation.
        CountedRecycler recycler = new CountedRecycler();
        BufferBuilder bufferBuilder =
                new BufferBuilder(allocateUnpooledSegment(BUFFER_SIZE), recycler);

        // and: One buffer consumer.
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        // when: Invoke the recycle of BufferBuilder.
        bufferBuilder.close();

        // then: Nothing happened because BufferBuilder has already consumer.
        assertThat(recycler.recycleInvocationCounter).isZero();

        // when: Close the consumer.
        bufferConsumer.close();

        // then: Recycling successfully finished.
        assertThat(recycler.recycleInvocationCounter).isOne();
    }

    @Test
    void trimToAvailableSize() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE);

        bufferBuilder.trim(BUFFER_SIZE / 2);
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE / 2);

        bufferBuilder.trim(0);
        assertThat(bufferBuilder.getMaxCapacity()).isZero();
    }

    @Test
    void trimToNegativeSize() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE);

        bufferBuilder.trim(-1);
        assertThat(bufferBuilder.getMaxCapacity()).isZero();
    }

    @Test
    void trimToSizeLessThanWritten() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE);

        bufferBuilder.append(toByteBuffer(1, 2, 3));

        bufferBuilder.trim(4);
        // Should be minimum possible size = 3 * int == 12.
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(12);
    }

    @Test
    void trimToSizeGreaterThanMax() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE);

        bufferBuilder.trim(BUFFER_SIZE + 1);
        assertThat(bufferBuilder.getMaxCapacity()).isEqualTo(BUFFER_SIZE);
    }

    private static void testIsFinished(int writes) {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        for (int i = 0; i < writes; i++) {
            assertThat(bufferBuilder.appendAndCommit(toByteBuffer(42))).isEqualTo(Integer.BYTES);
        }
        int expectedWrittenBytes = writes * Integer.BYTES;

        assertThat(bufferBuilder.isFinished()).isFalse();
        assertThat(bufferConsumer.isFinished()).isFalse();
        assertThat(bufferConsumer.getWrittenBytes()).isZero();

        bufferConsumer.build();
        assertThat(bufferBuilder.isFinished()).isFalse();
        assertThat(bufferConsumer.isFinished()).isFalse();
        assertThat(bufferConsumer.getWrittenBytes()).isEqualTo(expectedWrittenBytes);

        int actualWrittenBytes = bufferBuilder.finish();
        assertThat(actualWrittenBytes).isEqualTo(expectedWrittenBytes);
        assertThat(bufferBuilder.isFinished()).isTrue();
        assertThat(bufferConsumer.isFinished()).isFalse();
        assertThat(bufferConsumer.getWrittenBytes()).isEqualTo(expectedWrittenBytes);

        actualWrittenBytes = bufferBuilder.finish();
        assertThat(actualWrittenBytes).isEqualTo(expectedWrittenBytes);
        assertThat(bufferBuilder.isFinished()).isTrue();
        assertThat(bufferConsumer.isFinished()).isFalse();
        assertThat(bufferConsumer.getWrittenBytes()).isEqualTo(expectedWrittenBytes);

        assertThat(bufferConsumer.build().getSize()).isZero();
        assertThat(bufferBuilder.isFinished()).isTrue();
    }

    public static ByteBuffer toByteBuffer(int... data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * Integer.BYTES);
        byteBuffer.asIntBuffer().put(data);
        return byteBuffer;
    }

    private static void assertContent(BufferConsumer actualConsumer, int... expected) {
        assertThat(actualConsumer.isFinished()).isFalse();
        Buffer buffer = actualConsumer.build();
        assertThat(buffer.isRecycled()).isFalse();
        assertContent(buffer, FreeingBufferRecycler.INSTANCE, expected);
        assertThat(buffer.getSize()).isEqualTo(expected.length * Integer.BYTES);
        buffer.recycleBuffer();
    }

    public static void assertContent(
            Buffer actualBuffer, @Nullable BufferRecycler recycler, int... expected) {
        IntBuffer actualIntBuffer = actualBuffer.getNioBufferReadable().asIntBuffer();
        int[] actual = new int[actualIntBuffer.limit()];
        actualIntBuffer.get(actual);
        assertThat(actual).containsExactly(expected);

        if (recycler != null) {
            assertThat(actualBuffer.getRecycler()).isEqualTo(recycler);
        }
    }

    private static BufferBuilder createBufferBuilder() {
        return new BufferBuilder(
                allocateUnpooledSegment(BUFFER_SIZE), FreeingBufferRecycler.INSTANCE);
    }

    private static class CountedRecycler implements BufferRecycler {
        int recycleInvocationCounter;

        @Override
        public void recycle(MemorySegment memorySegment) {
            recycleInvocationCounter++;
            memorySegment.free();
        }
    }
}
