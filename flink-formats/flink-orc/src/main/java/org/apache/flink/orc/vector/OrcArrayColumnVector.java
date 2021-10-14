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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.types.logical.ArrayType;

import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;

/** This column vector is used to adapt hive's ListColumnVector to Flink's ArrayColumnVector. */
public class OrcArrayColumnVector extends AbstractOrcColumnVector
        implements org.apache.flink.table.data.vector.ArrayColumnVector {

    private final ListColumnVector hiveVector;
    private final ColumnVector flinkVector;

    public OrcArrayColumnVector(ListColumnVector hiveVector, ArrayType type) {
        super(hiveVector);
        this.hiveVector = hiveVector;
        this.flinkVector = createFlinkVector(hiveVector.child, type.getElementType());
    }

    @Override
    public ArrayData getArray(int i) {
        long offset = hiveVector.offsets[i];
        long length = hiveVector.lengths[i];
        return new ColumnarArrayData(flinkVector, (int) offset, (int) length);
    }
}
