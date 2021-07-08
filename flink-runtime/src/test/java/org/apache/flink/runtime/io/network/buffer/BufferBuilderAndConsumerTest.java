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

import org.junit.Test;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSingleBuffer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BufferBuilder}. */
public class BufferBuilderAndConsumerTest {
    private static final int BUFFER_INT_SIZE = 10;
    private static final int BUFFER_SIZE = BUFFER_INT_SIZE * Integer.BYTES;

    @Test
    public void referenceCounting() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        assertEquals(3 * Integer.BYTES, bufferBuilder.appendAndCommit(toByteBuffer(1, 2, 3)));

        bufferBuilder.close();

        Buffer buffer = bufferConsumer.build();
        assertFalse(buffer.isRecycled());
        buffer.recycleBuffer();
        assertFalse(buffer.isRecycled());
        bufferConsumer.close();
        assertTrue(buffer.isRecycled());
    }

    @Test
    public void append() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        int[] intsToWrite = new int[] {0, 1, 2, 3, 42};
        ByteBuffer bytesToWrite = toByteBuffer(intsToWrite);

        assertEquals(bytesToWrite.limit(), bufferBuilder.appendAndCommit(bytesToWrite));

        assertEquals(bytesToWrite.limit(), bytesToWrite.position());
        assertFalse(bufferBuilder.isFull());

        assertContent(bufferConsumer, intsToWrite);
    }

    @Test
    public void multipleAppends() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        bufferBuilder.appendAndCommit(toByteBuffer(0, 1));
        bufferBuilder.appendAndCommit(toByteBuffer(2));
        bufferBuilder.appendAndCommit(toByteBuffer(3, 42));

        assertContent(bufferConsumer, 0, 1, 2, 3, 42);
    }

    @Test
    public void multipleNotCommittedAppends() {
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
    public void appendOverSize() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
        ByteBuffer bytesToWrite = toByteBuffer(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 42);

        assertEquals(BUFFER_SIZE, bufferBuilder.appendAndCommit(bytesToWrite));

        assertTrue(bufferBuilder.isFull());
        assertContent(bufferConsumer, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bufferBuilder = createBufferBuilder();
        bufferConsumer = bufferBuilder.createBufferConsumer();
        assertEquals(Integer.BYTES, bufferBuilder.appendAndCommit(bytesToWrite));

        assertFalse(bufferBuilder.isFull());
        assertContent(bufferConsumer, 42);
    }

    @Test(expected = IllegalStateException.class)
    public void creatingBufferConsumerTwice() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        bufferBuilder.createBufferConsumer();
        bufferBuilder.createBufferConsumer();
    }

    @Test
    public void copy() {
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
    public void buildEmptyBuffer() {
        try (BufferBuilder bufferBuilder = createBufferBuilder()) {
            Buffer buffer = buildSingleBuffer(bufferBuilder);
            assertEquals(0, buffer.getSize());
            assertContent(buffer, FreeingBufferRecycler.INSTANCE);
        }
    }

    @Test
    public void buildingBufferMultipleTimes() {
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
    public void emptyIsFinished() {
        testIsFinished(0);
    }

    @Test
    public void partiallyFullIsFinished() {
        testIsFinished(BUFFER_INT_SIZE / 2);
    }

    @Test
    public void fullIsFinished() {
        testIsFinished(BUFFER_INT_SIZE);
    }

    @Test
    public void testWritableBytes() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        assertEquals(bufferBuilder.getMaxCapacity(), bufferBuilder.getWritableBytes());

        ByteBuffer byteBuffer = toByteBuffer(1, 2, 3);
        bufferBuilder.append(byteBuffer);
        assertEquals(
                bufferBuilder.getMaxCapacity() - byteBuffer.position(),
                bufferBuilder.getWritableBytes());

        assertEquals(
                bufferBuilder.getMaxCapacity() - byteBuffer.position(),
                bufferBuilder.getWritableBytes());
    }

    @Test
    public void testWritableBytesWhenFull() {
        BufferBuilder bufferBuilder = createBufferBuilder();
        bufferBuilder.append(toByteBuffer(new int[bufferBuilder.getMaxCapacity()]));
        assertEquals(0, bufferBuilder.getWritableBytes());
    }

    @Test
    public void recycleWithoutConsumer() {
        // given: Recycler with the counter of recycle invocation.
        CountedRecycler recycler = new CountedRecycler();
        BufferBuilder bufferBuilder =
                new BufferBuilder(allocateUnpooledSegment(BUFFER_SIZE), recycler);

        // when: Invoke the recycle.
        bufferBuilder.close();

        // then: Recycling successfully finished.
        assertEquals(1, recycler.recycleInvocationCounter);
    }

    @Test
    public void recycleConsumerAndBufferBuilder() {
        // given: Recycler with the counter of recycling invocation.
        CountedRecycler recycler = new CountedRecycler();
        BufferBuilder bufferBuilder =
                new BufferBuilder(allocateUnpooledSegment(BUFFER_SIZE), recycler);

        // and: One buffer consumer.
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        // when: Invoke the recycle of BufferBuilder.
        bufferBuilder.close();

        // then: Nothing happened because BufferBuilder has already consumer.
        assertEquals(0, recycler.recycleInvocationCounter);

        // when: Close the consumer.
        bufferConsumer.close();

        // then: Recycling successfully finished.
        assertEquals(1, recycler.recycleInvocationCounter);
    }

    private static void testIsFinished(int writes) {
        BufferBuilder bufferBuilder = createBufferBuilder();
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();

        for (int i = 0; i < writes; i++) {
            assertEquals(Integer.BYTES, bufferBuilder.appendAndCommit(toByteBuffer(42)));
        }
        int expectedWrittenBytes = writes * Integer.BYTES;

        assertFalse(bufferBuilder.isFinished());
        assertFalse(bufferConsumer.isFinished());
        assertEquals(0, bufferConsumer.getWrittenBytes());

        bufferConsumer.build();
        assertFalse(bufferBuilder.isFinished());
        assertFalse(bufferConsumer.isFinished());
        assertEquals(expectedWrittenBytes, bufferConsumer.getWrittenBytes());

        int actualWrittenBytes = bufferBuilder.finish();
        assertEquals(expectedWrittenBytes, actualWrittenBytes);
        assertTrue(bufferBuilder.isFinished());
        assertFalse(bufferConsumer.isFinished());
        assertEquals(expectedWrittenBytes, bufferConsumer.getWrittenBytes());

        actualWrittenBytes = bufferBuilder.finish();
        assertEquals(expectedWrittenBytes, actualWrittenBytes);
        assertTrue(bufferBuilder.isFinished());
        assertFalse(bufferConsumer.isFinished());
        assertEquals(expectedWrittenBytes, bufferConsumer.getWrittenBytes());

        assertEquals(0, bufferConsumer.build().getSize());
        assertTrue(bufferConsumer.isFinished());
    }

    public static ByteBuffer toByteBuffer(int... data) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * Integer.BYTES);
        byteBuffer.asIntBuffer().put(data);
        return byteBuffer;
    }

    private static void assertContent(BufferConsumer actualConsumer, int... expected) {
        assertFalse(actualConsumer.isFinished());
        Buffer buffer = actualConsumer.build();
        assertFalse(buffer.isRecycled());
        assertContent(buffer, FreeingBufferRecycler.INSTANCE, expected);
        assertEquals(expected.length * Integer.BYTES, buffer.getSize());
        buffer.recycleBuffer();
    }

    public static void assertContent(
            Buffer actualBuffer, @Nullable BufferRecycler recycler, int... expected) {
        IntBuffer actualIntBuffer = actualBuffer.getNioBufferReadable().asIntBuffer();
        int[] actual = new int[actualIntBuffer.limit()];
        actualIntBuffer.get(actual);
        assertArrayEquals(expected, actual);

        if (recycler != null) {
            assertEquals(recycler, actualBuffer.getRecycler());
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
