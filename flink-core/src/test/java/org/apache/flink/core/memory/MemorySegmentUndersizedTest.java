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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for undersized {@link MemorySegment} (in both heap and off-heap modes). */
class MemorySegmentUndersizedTest {

    @Test
    void testZeroSizeHeapSegment() {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(0);

        testZeroSizeBuffer(segment);
        testSegmentWithSizeLargerZero(segment);
    }

    @Test
    void testZeroSizeOffHeapSegment() {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledOffHeapMemory(0);

        testZeroSizeBuffer(segment);
        testSegmentWithSizeLargerZero(segment);
    }

    @Test
    void testZeroSizeOffHeapUnsafeSegment() {
        MemorySegment segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory(0);

        testZeroSizeBuffer(segment);
        testSegmentWithSizeLargerZero(segment);
    }

    @Test
    void testSizeOneHeapSegment() {
        testSegmentWithSizeLargerZero(MemorySegmentFactory.allocateUnpooledSegment(1));
    }

    @Test
    void testSizeOneOffHeapSegment() {
        testSegmentWithSizeLargerZero(MemorySegmentFactory.allocateUnpooledOffHeapMemory(1));
    }

    @Test
    void testSizeOneOffHeapUnsafeSegment() {
        testSegmentWithSizeLargerZero(MemorySegmentFactory.allocateOffHeapUnsafeMemory(1));
    }

    private static void testZeroSizeBuffer(MemorySegment segment) {
        // ------ bytes ------

        assertThatThrownBy(() -> segment.put(0, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(0)).isInstanceOf(IndexOutOfBoundsException.class);

        // ------ booleans ------

        assertThatThrownBy(() -> segment.putBoolean(0, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(0))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    private static void testSegmentWithSizeLargerZero(MemorySegment segment) {

        // ------ bytes ------
        assertThatThrownBy(() -> segment.put(1, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-1, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(8, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-8, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, (byte) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ booleans ------

        assertThatThrownBy(() -> segment.putBoolean(1, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putBoolean(-1, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putBoolean(8, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putBoolean(-8, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putBoolean(Integer.MAX_VALUE, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putBoolean(Integer.MIN_VALUE, true))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(-8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getBoolean(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ chars ------

        assertThatThrownBy(() -> segment.putChar(0, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(1, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(-1, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(8, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(-8, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(Integer.MAX_VALUE, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putChar(Integer.MIN_VALUE, 'a'))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(0)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(-1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(-8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getChar(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ shorts ------

        assertThatThrownBy(() -> segment.putShort(0, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(1, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(-1, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(8, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(-8, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(Integer.MAX_VALUE, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putShort(Integer.MIN_VALUE, (short) 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(0)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(-8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getShort(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ ints ------

        assertThatThrownBy(() -> segment.putInt(0, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(-1, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(8, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(-8, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(Integer.MAX_VALUE, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putInt(Integer.MIN_VALUE, 0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(0)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(-1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(-8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getInt(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ longs ------

        assertThatThrownBy(() -> segment.putLong(0, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(1, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(-1, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(8, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(-8, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(Integer.MAX_VALUE, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putLong(Integer.MIN_VALUE, 0L))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(0)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(-1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(-8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getLong(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ floats ------

        assertThatThrownBy(() -> segment.putFloat(0, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(1, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(-1, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(8, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(-8, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(Integer.MAX_VALUE, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putFloat(Integer.MIN_VALUE, 0.0f))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(0)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(1)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(8)).isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(-8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getFloat(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ doubles ------

        assertThatThrownBy(() -> segment.putDouble(0, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(1, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(-1, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(8, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(Integer.MAX_VALUE, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.putDouble(Integer.MIN_VALUE, 0.0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(0))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(-8))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(Integer.MAX_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.getDouble(Integer.MIN_VALUE))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ byte[] ------

        assertThatThrownBy(() -> segment.put(0, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(1, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-1, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(8, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-8, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(0, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(1, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-1, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(8, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-8, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, new byte[7]))
                .isInstanceOf(IndexOutOfBoundsException.class);
        // ------ ByteBuffer ------

        final ByteBuffer buf = ByteBuffer.allocate(7);
        final int numBytes = 3;

        assertThatThrownBy(() -> segment.put(0, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(1, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-1, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(8, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(-8, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MAX_VALUE, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(Integer.MIN_VALUE, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(0, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(1, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-1, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(8, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(-8, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MAX_VALUE, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(Integer.MIN_VALUE, buf, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        // ------ DataInput / DataOutput ------

        final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(new byte[20]));
        final DataOutput dataOutput = new DataOutputStream(new ByteArrayOutputStream());

        assertThatThrownBy(() -> segment.put(dataInput, 0, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, 1, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, -1, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, 8, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, -8, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, Integer.MAX_VALUE, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.put(dataInput, Integer.MIN_VALUE, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, 0, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, 1, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, -1, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, 8, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, -8, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, Integer.MAX_VALUE, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> segment.get(dataOutput, Integer.MIN_VALUE, numBytes))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }
}
