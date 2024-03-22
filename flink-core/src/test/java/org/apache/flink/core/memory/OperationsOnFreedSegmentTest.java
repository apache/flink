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

package org.apache.flink.core.memory;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Various tests with freed memory segments for {@link MemorySegment} (in both heap and off-heap
 * modes).
 */
class OperationsOnFreedSegmentTest {

    private static final int PAGE_SIZE = (int) ((Math.random() * 10000) + 1000);

    @Test
    void testSingleSegmentOperationsHeapSegment() throws Exception {
        for (MemorySegment segment : createTestSegments()) {
            testOpsOnFreedSegment(segment);
        }
    }

    @Test
    void testCompare() {
        MemorySegment aliveHeap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
        MemorySegment aliveOffHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);

        MemorySegment freedHeap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
        MemorySegment freedOffHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);
        freedHeap.free();
        freedOffHeap.free();

        MemorySegment[] alive = {aliveHeap, aliveOffHeap};
        MemorySegment[] free = {freedHeap, freedOffHeap};

        // alive with free
        for (MemorySegment seg1 : alive) {
            for (MemorySegment seg2 : free) {
                testCompare(seg1, seg2);
            }
        }

        // free with alive
        for (MemorySegment seg1 : free) {
            for (MemorySegment seg2 : alive) {
                testCompare(seg1, seg2);
            }
        }

        // free with free
        for (MemorySegment seg1 : free) {
            for (MemorySegment seg2 : free) {
                testCompare(seg1, seg2);
            }
        }
    }

    @Test
    void testCopyTo() {
        testAliveVsFree(this::testCopy);
    }

    @Test
    void testSwap() {
        testAliveVsFree(this::testSwap);
    }

    private static void testAliveVsFree(BiConsumer<MemorySegment, MemorySegment> testOperation) {
        MemorySegment[] alive = createTestSegments();
        MemorySegment[] free = createTestSegments();
        for (MemorySegment segment : free) {
            segment.free();
        }

        // alive with free
        for (MemorySegment seg1 : alive) {
            for (MemorySegment seg2 : free) {
                testOperation.accept(seg1, seg2);
            }
        }

        // free with alive
        for (MemorySegment seg1 : free) {
            for (MemorySegment seg2 : alive) {
                testOperation.accept(seg1, seg2);
            }
        }

        // free with free
        for (MemorySegment seg1 : free) {
            for (MemorySegment seg2 : free) {
                testOperation.accept(seg1, seg2);
            }
        }
    }

    private static MemorySegment[] createTestSegments() {
        MemorySegment heap = MemorySegmentFactory.wrap(new byte[PAGE_SIZE]);
        MemorySegment offHeap = MemorySegmentFactory.allocateUnpooledOffHeapMemory(PAGE_SIZE);
        MemorySegment offHeapUnsafe = MemorySegmentFactory.allocateOffHeapUnsafeMemory(PAGE_SIZE);

        return new MemorySegment[] {heap, offHeap, offHeapUnsafe};
    }

    private void testOpsOnFreedSegment(MemorySegment segment) throws Exception {
        segment.free();
        assertThat(segment.isFreed()).isTrue();

        // --------- bytes -----------

        assertThatThrownBy(() -> segment.get(0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.get(-1))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(1))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.get(segment.size()))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.get(-segment.size()))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(0, (byte) 0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.put(-1, (byte) 0))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(1, (byte) 0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.put(segment.size(), (byte) 0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.put(-segment.size(), (byte) 0))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, (byte) 0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, (byte) 0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        // --------- booleans -----------
        assertThatThrownBy(() -> segment.getBoolean(0))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.getBoolean(-1))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getBoolean(1))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.getBoolean(segment.size()))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.getBoolean(-segment.size()))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getBoolean(Integer.MAX_VALUE))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.getBoolean(Integer.MIN_VALUE))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.putBoolean(0, true))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.putBoolean(-1, true))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putBoolean(1, true))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.putBoolean(segment.size(), true))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.putBoolean(-segment.size(), true))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putBoolean(Integer.MAX_VALUE, true))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);
        assertThatThrownBy(() -> segment.putBoolean(Integer.MIN_VALUE, true))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        // --------- char -----------

        assertThatThrownBy(() -> segment.getChar(0)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.getChar(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getChar(1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.getChar(segment.size()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.getChar(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.getChar(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.getChar(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putChar(0, 'a')).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.putChar(-1, 'a'))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putChar(1, 'a')).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.putChar(segment.size(), 'a'))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.putChar(-segment.size(), 'a'))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> segment.putChar(Integer.MAX_VALUE, 'a'))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> segment.putChar(Integer.MIN_VALUE, 'a'))
                .isInstanceOf(IllegalStateException.class);

        // --------- short -----------
        assertThatThrownBy(() -> segment.getShort(0)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getShort(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(1)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getShort(segment.size()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getShort(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getShort(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putShort(0, (short) 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putShort(-1, (short) 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(1, (short) 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putShort(segment.size(), (short) 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putShort(-segment.size(), (short) 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(Integer.MAX_VALUE, (short) 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putShort(Integer.MIN_VALUE, (short) 42))
                .isInstanceOf(IllegalStateException.class);

        // --------- integer -----------
        assertThatThrownBy(() -> segment.getInt(0)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getInt(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(1)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getInt(segment.size()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getInt(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getInt(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putInt(0, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putInt(-1, 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(1, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putInt(segment.size(), 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putInt(-segment.size(), 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(Integer.MAX_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putInt(Integer.MIN_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);
        // --------- longs -----------

        assertThatThrownBy(() -> segment.getLong(0)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getLong(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(1)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getLong(segment.size()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getLong(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getLong(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putLong(0, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putLong(-1, 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(1, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putLong(segment.size(), 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putLong(-segment.size(), 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(Integer.MAX_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putLong(Integer.MIN_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        // --------- floats -----------

        assertThatThrownBy(() -> segment.getFloat(0)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getFloat(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(1)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getFloat(segment.size()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getFloat(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getFloat(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putFloat(0, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putFloat(-1, 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(1, 42)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putFloat(segment.size(), 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putFloat(-segment.size(), 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(Integer.MAX_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putFloat(Integer.MIN_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        // --------- doubles -----------

        assertThatThrownBy(() -> segment.getDouble(0)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getDouble(-1))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(1)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getDouble(segment.size()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getDouble(-segment.size()))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.getDouble(Integer.MIN_VALUE))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putDouble(0, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putDouble(-1, 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(1, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putDouble(segment.size(), 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putDouble(-segment.size(), 42))
                .isInstanceOfAny(IllegalStateException.class, IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(Integer.MAX_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> segment.putDouble(Integer.MIN_VALUE, 42))
                .isInstanceOf(IllegalStateException.class);

        // --------- byte[] -----------

        final byte[] array = new byte[55];

        assertThatThrownBy(() -> segment.get(0, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(-1, array, 3, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(1, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(segment.size(), array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(-segment.size(), array, 3, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(0, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(-1, array, 3, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(1, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(segment.size(), array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(-segment.size(), array, 3, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, array, 3, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        // --------- ByteBuffer -----------

        for (ByteBuffer bbuf :
                new ByteBuffer[] {ByteBuffer.allocate(55), ByteBuffer.allocateDirect(55)}) {

            assertThatThrownBy(() -> segment.get(0, bbuf, 17))
                    .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

            assertThatThrownBy(() -> segment.get(-1, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.get(1, bbuf, 17))
                    .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

            assertThatThrownBy(() -> segment.get(segment.size(), bbuf, 17))
                    .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

            assertThatThrownBy(() -> segment.get(-segment.size(), bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.put(0, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.put(-1, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.put(1, bbuf, 17))
                    .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

            assertThatThrownBy(() -> segment.put(segment.size(), bbuf, 17))
                    .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

            assertThatThrownBy(() -> segment.put(-segment.size(), bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);

            assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, bbuf, 17))
                    .isInstanceOfAny(
                            IllegalStateException.class,
                            NullPointerException.class,
                            IndexOutOfBoundsException.class);
        }

        // --------- Data Input / Output -----------

        final DataInput din = new DataInputStream(new ByteArrayInputStream(new byte[100]));
        final DataOutput dout = new DataOutputStream(new ByteArrayOutputStream());

        assertThatThrownBy(() -> segment.get(dout, 0, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(dout, -1, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dout, 1, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(dout, segment.size(), 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.get(dout, -segment.size(), 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dout, Integer.MAX_VALUE, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dout, Integer.MIN_VALUE, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(din, 0, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(din, -1, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(din, 1, 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(din, segment.size(), 17))
                .isInstanceOfAny(IllegalStateException.class, NullPointerException.class);

        assertThatThrownBy(() -> segment.put(din, -segment.size(), 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(din, Integer.MAX_VALUE, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(din, Integer.MIN_VALUE, 17))
                .isInstanceOfAny(
                        IllegalStateException.class,
                        NullPointerException.class,
                        IndexOutOfBoundsException.class);
    }

    private void testCompare(MemorySegment seg1, MemorySegment seg2) {
        int[] offsetsToTest = {
            0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        int[] lengthsToTest = {1, seg1.size(), Integer.MAX_VALUE};

        for (int off1 : offsetsToTest) {
            for (int off2 : offsetsToTest) {
                for (int len : lengthsToTest) {
                    assertThatThrownBy(() -> seg1.compare(seg2, off1, off2, len))
                            .isInstanceOfAny(
                                    IllegalStateException.class,
                                    IndexOutOfBoundsException.class,
                                    NullPointerException.class);
                }
            }
        }
    }

    private void testCopy(MemorySegment seg1, MemorySegment seg2) {
        int[] offsetsToTest = {
            0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        int[] lengthsToTest = {
            0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        for (int off1 : offsetsToTest) {
            for (int off2 : offsetsToTest) {
                for (int len : lengthsToTest) {
                    assertThatThrownBy(() -> seg1.copyTo(off1, seg2, off2, len))
                            .isInstanceOfAny(
                                    IllegalStateException.class,
                                    IndexOutOfBoundsException.class,
                                    NullPointerException.class);
                }
            }
        }
    }

    private void testSwap(MemorySegment seg1, MemorySegment seg2) {
        int[] offsetsToTest = {
            0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        int[] lengthsToTest = {
            0, 1, -1, seg1.size(), -seg1.size(), Integer.MAX_VALUE, Integer.MIN_VALUE
        };
        byte[] swapBuffer = new byte[seg1.size()];

        for (int off1 : offsetsToTest) {
            for (int off2 : offsetsToTest) {
                for (int len : lengthsToTest) {
                    assertThatThrownBy(() -> seg1.swapBytes(swapBuffer, seg2, off1, off2, len))
                            .isInstanceOfAny(
                                    IllegalStateException.class,
                                    IndexOutOfBoundsException.class,
                                    NullPointerException.class);
                }
            }
        }
    }
}
