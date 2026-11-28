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

package org.apache.flink.table.data.columnar;

import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.binary.BinaryGeographyData;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ColumnarArrayDataTest {

    private static final byte[] LITTLE_ENDIAN_POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    private static final byte[] BIG_ENDIAN_POINT_WKB =
            new byte[] {
                0,
                0,
                0,
                0,
                1,
                0x3F,
                (byte) 0xF0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0x40,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            };

    private static final byte[] MALFORMED_POINT_HEADER = new byte[] {1, 1, 0, 0, 0};

    @Test
    @DisplayName("getBinary() should work correctly for slices with position 0")
    void testGetBinaryWhenOffsetIsZero() {
        HeapBytesVector vector = new HeapBytesVector(2);
        byte[] sourceData = new byte[] {10, 20, 30, 40, 50};

        vector.appendBytes(0, sourceData, 0, 3);

        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 1);

        byte[] actual = arrayData.getBinary(0);

        byte[] expected = new byte[] {10, 20, 30};
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("getBinary() should return correct sub-array when slice position is non-zero")
    void testGetBinaryWhenPositionNonZero() {
        HeapBytesVector vector = new HeapBytesVector(3);

        byte[] dummyData = new byte[] {99, 99, 99, 99};
        vector.appendBytes(0, dummyData, 0, 4);

        byte[] sourceData1 = new byte[] {30, 40, 50, 60};
        vector.appendBytes(1, sourceData1, 0, 4);

        byte[] sourceData2 = new byte[] {70, 80, 90, 100};
        vector.appendBytes(2, sourceData2, 0, 4);

        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 3);
        assertThat(arrayData.getBinary(0)).isEqualTo(dummyData);
        assertThat(arrayData.getBinary(1)).isEqualTo(sourceData1);
        assertThat(arrayData.getBinary(2)).isEqualTo(sourceData2);
    }

    @Test
    void testGetGeographyReturnsBinaryViewForExactSlice() {
        HeapBytesVector vector = new HeapBytesVector(3);
        vector.appendBytes(0, new byte[] {99}, 0, 1);
        vector.appendBytes(1, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);
        vector.appendBytes(2, BIG_ENDIAN_POINT_WKB, 0, BIG_ENDIAN_POINT_WKB.length);

        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 3);

        GeographyData littleEndian = arrayData.getGeography(1);
        GeographyData bigEndian = arrayData.getGeography(2);

        assertThat(littleEndian).isInstanceOf(BinaryGeographyData.class);
        assertThat(bigEndian).isInstanceOf(BinaryGeographyData.class);
        assertThat(littleEndian.toBytes()).isEqualTo(LITTLE_ENDIAN_POINT_WKB);
        assertThat(bigEndian.toBytes()).isEqualTo(BIG_ENDIAN_POINT_WKB);
        assertThat(arrayData.getGeography(2).toBytes()).isEqualTo(BIG_ENDIAN_POINT_WKB);
    }

    @Test
    void testGetGeographyIsLazyAndAliasesBackingBuffer() {
        HeapBytesVector vector = new HeapBytesVector(2);
        vector.appendBytes(0, MALFORMED_POINT_HEADER, 0, MALFORMED_POINT_HEADER.length);
        vector.appendBytes(1, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);

        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 2);

        assertThatCode(() -> arrayData.getGeography(0)).doesNotThrowAnyException();
        GeographyData malformed = arrayData.getGeography(0);
        assertThat(malformed).isInstanceOf(BinaryGeographyData.class);
        assertThat(malformed.toBytes()).isEqualTo(MALFORMED_POINT_HEADER);

        GeographyData point = arrayData.getGeography(1);
        byte expectedByte = 0x55;
        vector.buffer[vector.start[1] + 5] = expectedByte;

        assertThat(point.toBytes()[5]).isEqualTo(expectedByte);
        assertThat(arrayData.getGeography(1).toBytes()[5]).isEqualTo(expectedByte);
    }

    @Test
    void testGetGeographyNullHandlingUsesArrayContract() {
        HeapBytesVector vector = new HeapBytesVector(2);
        vector.appendBytes(0, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);
        vector.setNullAt(1);

        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 2);

        assertThat(arrayData.isNullAt(0)).isFalse();
        assertThat(arrayData.isNullAt(1)).isTrue();
    }
}
