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

package org.apache.flink.orc.vector;

import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;

/** This column vector is used to adapt hive's StructColumnVector to Flink's RowColumnVector. */
public class OrcRowColumnVector extends AbstractOrcColumnVector
        implements org.apache.flink.table.data.vector.RowColumnVector {

    private final ColumnarRowData columnarRowData;

    public OrcRowColumnVector(StructColumnVector hiveVector, RowType type) {
        super(hiveVector);
        int len = hiveVector.fields.length;
        ColumnVector[] flinkVectors = new ColumnVector[len];
        for (int i = 0; i < len; i++) {
            flinkVectors[i] = createFlinkVector(hiveVector.fields[i], type.getTypeAt(i));
        }
        this.columnarRowData = new ColumnarRowData(new VectorizedColumnBatch(flinkVectors));
    }

    @Override
    public ColumnarRowData getRow(int i) {
        this.columnarRowData.setRowId(i);
        return this.columnarRowData;
    }
}
