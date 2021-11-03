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

package org.apache.flink.table.data.columnar.vector.heap;

import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.RowColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/** This class represents a nullable heap row column vector. */
public class HeapRowColumnVector extends AbstractHeapVector
        implements WritableColumnVector, RowColumnVector {

    public WritableColumnVector[] fields;

    public HeapRowColumnVector(int len, WritableColumnVector... fields) {
        super(len);
        this.fields = fields;
    }

    @Override
    public ColumnarRowData getRow(int i) {
        ColumnarRowData columnarRowData = new ColumnarRowData(new VectorizedColumnBatch(fields));
        columnarRowData.setRowId(i);
        return columnarRowData;
    }
}
