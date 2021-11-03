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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.vector.ArrayColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/** This class represents a nullable heap array column vector. */
public class HeapArrayVector extends AbstractHeapVector
        implements WritableColumnVector, ArrayColumnVector {

    public long[] offsets;
    public long[] lengths;
    public int childCount;
    public ColumnVector child;

    public HeapArrayVector(int len) {
        super(len);
        offsets = new long[len];
        lengths = new long[len];
    }

    public HeapArrayVector(int len, ColumnVector vector) {
        super(len);
        offsets = new long[len];
        lengths = new long[len];
        this.child = vector;
    }

    @Override
    public ArrayData getArray(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarArrayData(child, (int) offset, (int) length);
    }
}
