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

import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.columnar.ColumnarMapData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.MapColumnVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;

/** This class represents a nullable heap map column vector. */
public class HeapMapColumnVector extends AbstractHeapVector
        implements WritableColumnVector, MapColumnVector {

    public long[] offsets;
    public long[] lengths;
    public int childCount;
    public ColumnVector keys;
    public ColumnVector values;

    public HeapMapColumnVector(int len, ColumnVector keys, ColumnVector values) {
        super(len);
        childCount = 0;
        offsets = new long[len];
        lengths = new long[len];
        this.keys = keys;
        this.values = values;
    }

    @Override
    public MapData getMap(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarMapData(keys, values, (int) offset, (int) length);
    }
}
