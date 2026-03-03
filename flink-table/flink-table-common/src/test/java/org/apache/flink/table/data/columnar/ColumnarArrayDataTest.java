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

import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnarArrayDataTest {

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

        // Append a DUMMY element first
        byte[] dummyData = new byte[] {99, 99, 99, 99};
        vector.appendBytes(0, dummyData, 0, 4);

        // Append the REAL data
        byte[] sourceData1 = new byte[] {30, 40, 50, 60};
        vector.appendBytes(1, sourceData1, 0, 4);

        byte[] sourceData2 = new byte[] {70, 80, 90, 100};
        vector.appendBytes(2, sourceData2, 0, 4);

        // Create a ColumnarArrayData that wraps the entire vector
        ColumnarArrayData arrayData = new ColumnarArrayData(vector, 0, 1);
        assertThat(arrayData.getBinary(0)).isEqualTo(dummyData);
        assertThat(arrayData.getBinary(1)).isEqualTo(sourceData1);
        assertThat(arrayData.getBinary(2)).isEqualTo(sourceData2);
    }
}
