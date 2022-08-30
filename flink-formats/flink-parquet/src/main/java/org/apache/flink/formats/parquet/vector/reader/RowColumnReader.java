/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.columnar.vector.heap.HeapRowVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

import java.io.IOException;
import java.util.List;

/** Row {@link ColumnReader}. */
public class RowColumnReader implements ColumnReader<WritableColumnVector> {

    private final List<ColumnReader> fieldReaders;

    public RowColumnReader(List<ColumnReader> fieldReaders) {
        this.fieldReaders = fieldReaders;
    }

    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        HeapRowVector rowVector = (HeapRowVector) vector;
        WritableColumnVector[] vectors = rowVector.getFields();
        // row vector null array
        boolean[] isNulls = new boolean[readNumber];
        for (int i = 0; i < vectors.length; i++) {
            fieldReaders.get(i).readToVector(readNumber, vectors[i]);

            for (int j = 0; j < readNumber; j++) {
                if (i == 0) {
                    isNulls[j] = vectors[i].isNullAt(j);
                } else {
                    isNulls[j] = isNulls[j] && vectors[i].isNullAt(j);
                }
                if (i == vectors.length - 1 && isNulls[j]) {
                    // rowColumnVector[j] is null only when all fields[j] of rowColumnVector[j] is
                    // null
                    rowVector.setNullAt(j);
                }
            }
        }
    }
}
