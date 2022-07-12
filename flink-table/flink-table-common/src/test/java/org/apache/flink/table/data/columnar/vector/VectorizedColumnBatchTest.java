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

package org.apache.flink.table.data.columnar.vector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.utils.DateTimeUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link VectorizedColumnBatch}. */
class VectorizedColumnBatchTest {

    private static final int VECTOR_SIZE = 1024;
    private static final int ARRAY_SIZE = 3;

    @Test
    void testTyped() throws IOException {
        HeapBooleanVector col0 = new HeapBooleanVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col0.vector[i] = i % 2 == 0;
        }

        HeapBytesVector col1 = new HeapBytesVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            byte[] bytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
            col1.appendBytes(i, bytes, 0, bytes.length);
        }

        HeapByteVector col2 = new HeapByteVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col2.vector[i] = (byte) i;
        }

        HeapDoubleVector col3 = new HeapDoubleVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col3.vector[i] = i;
        }

        HeapFloatVector col4 = new HeapFloatVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col4.vector[i] = i;
        }

        HeapIntVector col5 = new HeapIntVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col5.vector[i] = i;
        }

        HeapLongVector col6 = new HeapLongVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col6.vector[i] = i;
        }

        HeapShortVector col7 = new HeapShortVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col7.vector[i] = (short) i;
        }

        // The precision of Timestamp in parquet should be one of MILLIS, MICROS or NANOS.
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
        //
        // For MILLIS, the underlying INT64 holds milliseconds
        // For MICROS, the underlying INT64 holds microseconds
        // For NANOS, the underlying INT96 holds nanoOfDay(8 bytes) and julianDay(4 bytes)
        long[] vector8 = new long[VECTOR_SIZE];
        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector8[i] = i;
        }
        TimestampColumnVector col8 =
                new TimestampColumnVector() {
                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }

                    @Override
                    public TimestampData getTimestamp(int i, int precision) {
                        return TimestampData.fromEpochMillis(vector8[i]);
                    }
                };

        long[] vector9 = new long[VECTOR_SIZE];
        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector9[i] = i * 1000;
        }
        TimestampColumnVector col9 =
                new TimestampColumnVector() {
                    @Override
                    public TimestampData getTimestamp(int i, int precision) {
                        long microseconds = vector9[i];
                        return TimestampData.fromEpochMillis(
                                microseconds / 1000, (int) (microseconds % 1000) * 1000);
                    }

                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }
                };

        HeapBytesVector vector10 = new HeapBytesVector(VECTOR_SIZE);
        {
            int nanosecond = 123456789;
            int start = 0;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0; i < VECTOR_SIZE; i++) {
                byte[] bytes = new byte[12];
                long l = i * 1000000000L + nanosecond; // i means second
                for (int j = 0; j < 8; j++) {
                    bytes[7 - j] = (byte) l;
                    l >>>= 8;
                }
                int n = 2440588; // Epoch Julian
                for (int j = 0; j < 4; j++) {
                    bytes[11 - j] = (byte) n;
                    n >>>= 8;
                }

                vector10.start[i] = start;
                vector10.length[i] = 12;
                start += 12;
                out.write(bytes);
            }
            vector10.buffer = out.toByteArray();
        }

        TimestampColumnVector col10 =
                new TimestampColumnVector() {
                    @Override
                    public TimestampData getTimestamp(int colId, int precision) {
                        byte[] bytes = vector10.getBytes(colId).getBytes();
                        assertThat(bytes).hasSize(12);
                        long nanoOfDay = 0;
                        for (int i = 0; i < 8; i++) {
                            nanoOfDay <<= 8;
                            nanoOfDay |= (bytes[i] & (0xff));
                        }
                        int julianDay = 0;
                        for (int i = 8; i < 12; i++) {
                            julianDay <<= 8;
                            julianDay |= (bytes[i] & (0xff));
                        }
                        long millisecond =
                                (julianDay - DateTimeUtils.EPOCH_JULIAN)
                                                * DateTimeUtils.MILLIS_PER_DAY
                                        + nanoOfDay / 1000000;
                        int nanoOfMillisecond = (int) (nanoOfDay % 1000000);
                        return TimestampData.fromEpochMillis(millisecond, nanoOfMillisecond);
                    }

                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }
                };

        long[] vector11 = new long[VECTOR_SIZE];
        DecimalColumnVector col11 =
                new DecimalColumnVector() {

                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }

                    @Override
                    public DecimalData getDecimal(int i, int precision, int scale) {
                        return DecimalData.fromUnscaledLong(vector11[i], precision, scale);
                    }
                };
        for (int i = 0; i < VECTOR_SIZE; i++) {
            vector11[i] = i;
        }

        HeapIntVector col12Data = new HeapIntVector(VECTOR_SIZE * ARRAY_SIZE);
        for (int i = 0; i < VECTOR_SIZE * ARRAY_SIZE; i++) {
            col12Data.vector[i] = i;
        }
        ArrayColumnVector col12 =
                new ArrayColumnVector() {

                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }

                    @Override
                    public ArrayData getArray(int i) {
                        return new ColumnarArrayData(col12Data, i * ARRAY_SIZE, ARRAY_SIZE);
                    }
                };

        VectorizedColumnBatch batch =
                new VectorizedColumnBatch(
                        new ColumnVector[] {
                            col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
                            col11, col12
                        });
        batch.setNumRows(VECTOR_SIZE);

        for (int i = 0; i < batch.getNumRows(); i++) {
            ColumnarRowData row = new ColumnarRowData(batch, i);
            assertThat(row.getBoolean(0)).isEqualTo(i % 2 == 0);
            assertThat(row.getString(1).toString()).isEqualTo(String.valueOf(i));
            assertThat(row.getByte(2)).isEqualTo((byte) i);
            assertThat(row.getDouble(3)).isEqualTo(i);
            assertThat((float) i).isEqualTo(row.getFloat(4));
            assertThat(row.getInt(5)).isEqualTo(i);
            assertThat(row.getLong(6)).isEqualTo(i);
            assertThat(row.getShort(7)).isEqualTo((short) i);
            assertThat(row.getTimestamp(8, 3).getMillisecond()).isEqualTo(i);
            assertThat(row.getTimestamp(9, 6).getMillisecond()).isEqualTo(i);
            assertThat(row.getTimestamp(10, 9).getMillisecond()).isEqualTo(i * 1000L + 123);
            assertThat(row.getTimestamp(10, 9).getNanoOfMillisecond()).isEqualTo(456789);
            assertThat(row.getDecimal(11, 10, 0).toUnscaledLong()).isEqualTo(i);
            for (int j = 0; j < ARRAY_SIZE; j++) {
                assertThat(row.getArray(12).getInt(j)).isEqualTo(i * ARRAY_SIZE + j);
            }
        }

        assertThat(batch.getNumRows()).isEqualTo(VECTOR_SIZE);
    }

    @Test
    void testNull() {
        // all null
        HeapIntVector col0 = new HeapIntVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            col0.setNullAt(i);
        }

        // some null
        HeapIntVector col1 = new HeapIntVector(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            if (i % 2 == 0) {
                col1.setNullAt(i);
            } else {
                col1.vector[i] = i;
            }
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {col0, col1});

        for (int i = 0; i < VECTOR_SIZE; i++) {
            ColumnarRowData row = new ColumnarRowData(batch, i);
            assertThat(row.isNullAt(0)).isTrue();
            if (i % 2 == 0) {
                assertThat(row.isNullAt(1)).isTrue();
            } else {
                assertThat(i).isEqualTo(row.getInt(1));
            }
        }
    }

    @Test
    void testDictionary() {
        // all null
        HeapIntVector col = new HeapIntVector(VECTOR_SIZE);
        Integer[] dict = new Integer[2];
        dict[0] = 1998;
        dict[1] = 9998;
        col.setDictionary(new ColumnVectorTest.TestDictionary(dict));
        HeapIntVector heapIntVector = col.reserveDictionaryIds(VECTOR_SIZE);
        for (int i = 0; i < VECTOR_SIZE; i++) {
            heapIntVector.vector[i] = i % 2 == 0 ? 0 : 1;
        }

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {col});

        for (int i = 0; i < VECTOR_SIZE; i++) {
            ColumnarRowData row = new ColumnarRowData(batch, i);
            if (i % 2 == 0) {
                assertThat(1998).isEqualTo(row.getInt(0));
            } else {
                assertThat(9998).isEqualTo(row.getInt(0));
            }
        }
    }
}
