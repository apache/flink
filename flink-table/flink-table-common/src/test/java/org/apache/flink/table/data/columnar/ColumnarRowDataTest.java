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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryGeographyData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.types.logical.GeographyType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ColumnarRowDataTest {

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
    void testGetGeographyReturnsBinaryViewForRows() {
        HeapBytesVector vector = new HeapBytesVector(3);
        vector.appendBytes(0, new byte[] {42}, 0, 1);
        vector.appendBytes(1, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);
        vector.appendBytes(2, BIG_ENDIAN_POINT_WKB, 0, BIG_ENDIAN_POINT_WKB.length);

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        ColumnarRowData firstRow = new ColumnarRowData(batch, 1);
        ColumnarRowData secondRow = new ColumnarRowData(batch, 2);

        GeographyData first = firstRow.getGeography(0);
        GeographyData second = secondRow.getGeography(0);

        assertThat(first).isInstanceOf(BinaryGeographyData.class);
        assertThat(second).isInstanceOf(BinaryGeographyData.class);
        assertThat(first.toBytes()).isEqualTo(LITTLE_ENDIAN_POINT_WKB);
        assertThat(second.toBytes()).isEqualTo(BIG_ENDIAN_POINT_WKB);
        assertThat(secondRow.getGeography(0).toBytes()).isEqualTo(BIG_ENDIAN_POINT_WKB);
    }

    @Test
    void testGetGeographyIsLazyAndAliasesBackingBuffer() {
        HeapBytesVector vector = new HeapBytesVector(2);
        vector.appendBytes(0, MALFORMED_POINT_HEADER, 0, MALFORMED_POINT_HEADER.length);
        vector.appendBytes(1, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        ColumnarRowData malformedRow = new ColumnarRowData(batch, 0);
        ColumnarRowData pointRow = new ColumnarRowData(batch, 1);

        assertThatCode(() -> malformedRow.getGeography(0)).doesNotThrowAnyException();
        GeographyData malformed = malformedRow.getGeography(0);
        assertThat(malformed).isInstanceOf(BinaryGeographyData.class);
        assertThat(malformed.toBytes()).isEqualTo(MALFORMED_POINT_HEADER);

        GeographyData point = pointRow.getGeography(0);
        byte expectedByte = 0x33;
        vector.buffer[vector.start[1] + 5] = expectedByte;

        assertThat(point.toBytes()[5]).isEqualTo(expectedByte);
        assertThat(pointRow.getGeography(0).toBytes()[5]).isEqualTo(expectedByte);
    }

    @Test
    void testGetGeographyNullHandlingUsesFieldGetterContract() {
        HeapBytesVector vector = new HeapBytesVector(2);
        vector.appendBytes(0, LITTLE_ENDIAN_POINT_WKB, 0, LITTLE_ENDIAN_POINT_WKB.length);
        vector.setNullAt(1);

        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        ColumnarRowData nullRow = new ColumnarRowData(batch, 1);
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(new GeographyType(), 0);

        assertThat(nullRow.isNullAt(0)).isTrue();
        assertThat(fieldGetter.getFieldOrNull(nullRow)).isNull();
    }
}
