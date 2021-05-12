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

import org.apache.flink.table.data.ColumnarMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.vector.ColumnVector;
import org.apache.flink.table.types.logical.MapType;

import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;

/** This column vector is used to adapt hive's MapColumnVector to Flink's MapColumnVector. */
public class OrcMapColumnVector extends AbstractOrcColumnVector
        implements org.apache.flink.table.data.vector.MapColumnVector {

    private final MapColumnVector hiveVector;
    private final ColumnVector keyFlinkVector;
    private final ColumnVector valueFlinkVector;

    public OrcMapColumnVector(MapColumnVector hiveVector, MapType type) {
        super(hiveVector);
        this.hiveVector = hiveVector;
        this.keyFlinkVector = createFlinkVector(hiveVector.keys, type.getKeyType());
        this.valueFlinkVector = createFlinkVector(hiveVector.values, type.getValueType());
    }

    @Override
    public MapData getMap(int i) {
        long offset = hiveVector.offsets[i];
        long length = hiveVector.lengths[i];
        return new ColumnarMapData(keyFlinkVector, valueFlinkVector, (int) offset, (int) length);
    }
}
