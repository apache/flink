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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.vector.ArrayColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/** This class represents a nullable heap array column vector. */
@Internal
public class HeapArrayVector extends AbstractHeapVector
        implements WritableColumnVector, ArrayColumnVector {

    private long[] offsets;
    private long[] lengths;
    private int size;
    private ColumnVector child;

    public HeapArrayVector(int len) {
        super(len);
        this.offsets = new long[len];
        this.lengths = new long[len];
    }

    public HeapArrayVector(int len, ColumnVector vector) {
        super(len);
        this.offsets = new long[len];
        this.lengths = new long[len];
        this.child = vector;
    }

    public long[] getOffsets() {
        return offsets;
    }

    public void setOffsets(long[] offsets) {
        this.offsets = offsets;
    }

    public long[] getLengths() {
        return lengths;
    }

    public void setLengths(long[] lengths) {
        this.lengths = lengths;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public ColumnVector getChild() {
        return child;
    }

    public void setChild(ColumnVector child) {
        this.child = child;
    }

    @Override
    public ArrayData getArray(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarArrayData(child, (int) offset, (int) length);
    }
}
