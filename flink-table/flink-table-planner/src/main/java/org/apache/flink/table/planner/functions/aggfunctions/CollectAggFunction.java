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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in COLLECT aggregate function. */
@Internal
public final class CollectAggFunction<T>
        extends BuiltInAggregateFunction<MapData, CollectAggFunction.CollectAccumulator<T>> {

    private static final long serialVersionUID = -5860934997657147836L;

    private transient DataType elementDataType;

    public CollectAggFunction(LogicalType elementType) {
        this.elementDataType = toInternalDataType(elementType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(elementDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                CollectAccumulator.class,
                DataTypes.FIELD(
                        "map",
                        MapView.newMapViewDataType(elementDataType.notNull(), DataTypes.INT())));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.MULTISET(elementDataType).bridgedTo(MapData.class);
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for COLLECT. */
    public static class CollectAccumulator<T> {
        public MapView<T, Integer> map;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CollectAccumulator<?> that = (CollectAccumulator<?>) o;
            return Objects.equals(map, that.map);
        }
    }

    public CollectAccumulator<T> createAccumulator() {
        final CollectAccumulator<T> acc = new CollectAccumulator<>();
        acc.map = new MapView<>();
        return acc;
    }

    public void resetAccumulator(CollectAccumulator<T> accumulator) {
        accumulator.map.clear();
    }

    public void accumulate(CollectAccumulator<T> accumulator, T value) throws Exception {
        if (value != null) {
            Integer count = accumulator.map.get(value);
            if (count != null) {
                accumulator.map.put(value, count + 1);
            } else {
                accumulator.map.put(value, 1);
            }
        }
    }

    public void retract(CollectAccumulator<T> accumulator, T value) throws Exception {
        if (value != null) {
            Integer count = accumulator.map.get(value);
            if (count != null) {
                if (count == 1) {
                    accumulator.map.remove(value);
                } else {
                    accumulator.map.put(value, count - 1);
                }
            } else {
                accumulator.map.put(value, -1);
            }
        }
    }

    public void merge(CollectAccumulator<T> accumulator, Iterable<CollectAccumulator<T>> others)
            throws Exception {
        for (CollectAccumulator<T> other : others) {
            for (Map.Entry<T, Integer> entry : other.map.entries()) {
                T key = entry.getKey();
                Integer newCount = entry.getValue();
                Integer oldCount = accumulator.map.get(key);
                if (oldCount == null) {
                    accumulator.map.put(key, newCount);
                } else {
                    accumulator.map.put(key, oldCount + newCount);
                }
            }
        }
    }

    @Override
    public MapData getValue(CollectAccumulator<T> accumulator) {
        return new GenericMapData(accumulator.map.getMap());
    }
}
