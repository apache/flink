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

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.columnar.ColumnarMapData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.MapColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.MapVector;

/** Arrow column vector for Map. */
@Internal
public final class ArrowMapColumnVector implements MapColumnVector {

    /** Container which is used to store the map values of a column to read. */
    private final MapVector mapVector;

    private final ColumnVector keyVector;

    private final ColumnVector valueVector;

    public ArrowMapColumnVector(
            MapVector mapVector, ColumnVector keyVector, ColumnVector valueVector) {
        this.mapVector = Preconditions.checkNotNull(mapVector);
        this.keyVector = Preconditions.checkNotNull(keyVector);
        this.valueVector = Preconditions.checkNotNull(valueVector);
    }

    @Override
    public MapData getMap(int i) {
        int index = i * MapVector.OFFSET_WIDTH;
        int offset = mapVector.getOffsetBuffer().getInt(index);
        int numElements = mapVector.getInnerValueCountAt(i);
        return new ColumnarMapData(keyVector, valueVector, offset, numElements);
    }

    @Override
    public boolean isNullAt(int i) {
        return mapVector.isNull(i);
    }
}
