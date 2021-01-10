/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.vector.heap.HeapByteVector;
import org.apache.flink.table.data.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.vector.heap.HeapIntVector;
import org.apache.flink.table.data.vector.heap.HeapLongVector;
import org.apache.flink.table.data.vector.heap.HeapShortVector;
import org.apache.flink.table.data.vector.heap.HeapTimestampVector;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.BYTE_ARRAY_OFFSET;
import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.DOUBLE_ARRAY_OFFSET;
import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.FLOAT_ARRAY_OFFSET;
import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.INT_ARRAY_OFFSET;
import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.LONG_ARRAY_OFFSET;
import static org.apache.flink.table.data.vector.heap.AbstractHeapVector.UNSAFE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test {@link ColumnVector}. */
public class ColumnVectorTest {

    private static final int SIZE = 10;

    @Test
    public void testNulls() {
        HeapBooleanVector vector = new HeapBooleanVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            if (i % 2 == 0) {
                vector.setNullAt(i);
            }
        }
        for (int i = 0; i < SIZE; i++) {
            if (i % 2 == 0) {
                assertTrue(vector.isNullAt(i));
            } else {
                assertFalse(vector.isNullAt(i));
            }
        }

        vector.fillWithNulls();
        for (int i = 0; i < SIZE; i++) {
            assertTrue(vector.isNullAt(i));
        }

        vector.reset();
        for (int i = 0; i < SIZE; i++) {
            assertFalse(vector.isNullAt(i));
        }

        vector.setNulls(0, SIZE / 2);
        for (int i = 0; i < SIZE / 2; i++) {
            assertTrue(vector.isNullAt(i));
        }
    }

    @Test
    public void testBoolean() {
        HeapBooleanVector vector = new HeapBooleanVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setBoolean(i, i % 2 == 0);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i % 2 == 0, vector.getBoolean(i));
        }

        vector.fill(true);
        for (int i = 0; i < SIZE; i++) {
            assertTrue(vector.getBoolean(i));
        }
    }

    @Test
    public void testByte() {
        HeapByteVector vector = new HeapByteVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setByte(i, (byte) i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getByte(i));
        }

        vector.fill((byte) 22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getByte(i));
        }

        vector.setDictionary(new TestDictionary(IntStream.range(0, SIZE).boxed().toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getByte(i));
        }
    }

    @Test
    public void testShort() {
        HeapShortVector vector = new HeapShortVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setShort(i, (short) i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getShort(i));
        }

        vector.fill((short) 22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getShort(i));
        }

        vector.setDictionary(new TestDictionary(IntStream.range(0, SIZE).boxed().toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getShort(i));
        }
    }

    @Test
    public void testInt() {
        HeapIntVector vector = new HeapIntVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setInt(i, i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getInt(i));
        }

        vector.fill(22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getInt(i));
        }

        vector = new HeapIntVector(SIZE);
        vector.setInts(0, SIZE, 22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getInt(i));
        }

        vector.setDictionary(new TestDictionary(IntStream.range(0, SIZE).boxed().toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getInt(i));
        }

        int[] ints = IntStream.range(0, SIZE).toArray();
        byte[] binary = new byte[SIZE * 8];
        UNSAFE.copyMemory(ints, INT_ARRAY_OFFSET, binary, BYTE_ARRAY_OFFSET, binary.length);
        vector = new HeapIntVector(SIZE);
        vector.setIntsFromBinary(0, SIZE, binary, 0);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getInt(i));
        }
    }

    @Test
    public void testLong() {
        HeapLongVector vector = new HeapLongVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setLong(i, i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getLong(i));
        }

        vector.fill(22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getLong(i));
        }

        vector.setDictionary(new TestDictionary(LongStream.range(0, SIZE).boxed().toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getLong(i));
        }

        long[] longs = LongStream.range(0, SIZE).toArray();
        byte[] binary = new byte[SIZE * 8];
        UNSAFE.copyMemory(longs, LONG_ARRAY_OFFSET, binary, BYTE_ARRAY_OFFSET, binary.length);
        vector = new HeapLongVector(SIZE);
        vector.setLongsFromBinary(0, SIZE, binary, 0);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getLong(i));
        }
    }

    @Test
    public void testFloat() {
        HeapFloatVector vector = new HeapFloatVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setFloat(i, i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getFloat(i), 0);
        }

        vector.fill(22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getFloat(i), 0);
        }

        vector.setDictionary(
                new TestDictionary(
                        LongStream.range(0, SIZE).boxed().map(Number::floatValue).toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getFloat(i), 0);
        }

        float[] floats = new float[SIZE];
        for (int i = 0; i < SIZE; i++) {
            floats[i] = i;
        }
        byte[] binary = new byte[SIZE * 4];
        UNSAFE.copyMemory(floats, FLOAT_ARRAY_OFFSET, binary, BYTE_ARRAY_OFFSET, binary.length);
        vector = new HeapFloatVector(SIZE);
        vector.setFloatsFromBinary(0, SIZE, binary, 0);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getFloat(i), 0);
        }
    }

    @Test
    public void testDouble() {
        HeapDoubleVector vector = new HeapDoubleVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setDouble(i, i);
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getDouble(i), 0);
        }

        vector.fill(22);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(22, vector.getDouble(i), 0);
        }

        vector.setDictionary(
                new TestDictionary(
                        LongStream.range(0, SIZE).boxed().map(Number::doubleValue).toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getDouble(i), 0);
        }

        double[] doubles =
                LongStream.range(0, SIZE).boxed().mapToDouble(Number::doubleValue).toArray();
        byte[] binary = new byte[SIZE * 8];
        UNSAFE.copyMemory(doubles, DOUBLE_ARRAY_OFFSET, binary, BYTE_ARRAY_OFFSET, binary.length);
        vector = new HeapDoubleVector(SIZE);
        vector.setDoublesFromBinary(0, SIZE, binary, 0);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, vector.getDouble(i), 0);
        }
    }

    private byte[] produceBytes(int i) {
        return (i + "").getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testBytes() {
        HeapBytesVector vector = new HeapBytesVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            byte[] bytes = produceBytes(i);
            vector.appendBytes(i, bytes, 0, bytes.length);
        }
        for (int i = 0; i < SIZE; i++) {
            assertArrayEquals(produceBytes(i), vector.getBytes(i).getBytes());
        }
        vector.reset();
        for (int i = 0; i < SIZE; i++) {
            byte[] bytes = produceBytes(i);
            vector.appendBytes(i, bytes, 0, bytes.length);
        }
        for (int i = 0; i < SIZE; i++) {
            assertArrayEquals(produceBytes(i), vector.getBytes(i).getBytes());
        }

        vector.fill(produceBytes(22));
        for (int i = 0; i < SIZE; i++) {
            assertArrayEquals(produceBytes(22), vector.getBytes(i).getBytes());
        }

        vector.setDictionary(
                new TestDictionary(
                        IntStream.range(0, SIZE).mapToObj(this::produceBytes).toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertArrayEquals(produceBytes(i), vector.getBytes(i).getBytes());
        }
    }

    @Test
    public void testTimestamp() {
        HeapTimestampVector vector = new HeapTimestampVector(SIZE);

        for (int i = 0; i < SIZE; i++) {
            vector.setTimestamp(i, TimestampData.fromEpochMillis(i, i));
        }
        for (int i = 0; i < SIZE; i++) {
            assertEquals(TimestampData.fromEpochMillis(i, i), vector.getTimestamp(i, 9));
        }

        vector.fill(TimestampData.fromEpochMillis(22, 22));
        for (int i = 0; i < SIZE; i++) {
            assertEquals(TimestampData.fromEpochMillis(22, 22), vector.getTimestamp(i, 9));
        }

        vector.setDictionary(
                new TestDictionary(
                        IntStream.range(0, SIZE)
                                .mapToObj(i -> TimestampData.fromEpochMillis(i, i))
                                .toArray()));
        setRangeDictIds(vector);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(TimestampData.fromEpochMillis(i, i), vector.getTimestamp(i, 9));
        }
    }

    @Test
    public void testReserveDictIds() {
        HeapIntVector vector = new HeapIntVector(SIZE);
        assertTrue(vector.reserveDictionaryIds(2).vector.length >= 2);
        assertTrue(vector.reserveDictionaryIds(5).vector.length >= 5);
        assertTrue(vector.reserveDictionaryIds(2).vector.length >= 2);
    }

    private void setRangeDictIds(WritableColumnVector vector) {
        vector.reserveDictionaryIds(SIZE).setInts(0, SIZE, IntStream.range(0, SIZE).toArray(), 0);
    }

    /** Test Dictionary. Just return Object value. */
    static final class TestDictionary implements Dictionary {
        private Object[] intDictionary;

        TestDictionary(Object[] dictionary) {
            this.intDictionary = dictionary;
        }

        @Override
        public int decodeToInt(int id) {
            return (int) intDictionary[id];
        }

        @Override
        public long decodeToLong(int id) {
            return (long) intDictionary[id];
        }

        @Override
        public float decodeToFloat(int id) {
            return (float) intDictionary[id];
        }

        @Override
        public double decodeToDouble(int id) {
            return (double) intDictionary[id];
        }

        @Override
        public byte[] decodeToBinary(int id) {
            return (byte[]) intDictionary[id];
        }

        @Override
        public TimestampData decodeToTimestamp(int id) {
            return (TimestampData) intDictionary[id];
        }
    }
}
