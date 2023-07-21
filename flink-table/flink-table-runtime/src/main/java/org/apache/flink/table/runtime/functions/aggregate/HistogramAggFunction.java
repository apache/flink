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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** @param <T> */
public class HistogramAggFunction<T>
        extends BuiltInAggregateFunction<MapData, HistogramAggFunction.HistogramAccumulator<T>> {

    private final transient DataType elementDataType;

    private static final int LIMIT = 1000;

    public HistogramAggFunction(LogicalType elementType) {
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
                HistogramAccumulator.class,
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
    public static class HistogramAccumulator<T> {
        public MapView<T, Integer> map;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            HistogramAccumulator<?> that = (HistogramAccumulator<?>) o;
            return Objects.equals(map, that.map);
        }
    }

    @Override
    public HistogramAccumulator<T> createAccumulator() {
        final HistogramAccumulator<T> acc = new HistogramAccumulator<>();
        acc.map = new MapView<>();
        return acc;
    }

    public void resetAccumulator(HistogramAccumulator<T> accumulator) {
        accumulator.map.clear();
    }

    public void accumulate(HistogramAccumulator<T> accumulator, T value) throws Exception {
        if (accumulator.map.size() < LIMIT && value != null) {
            Integer count = accumulator.map.get(value);
            if (count != null) {
                accumulator.map.put(value, count + 1);
            } else {
                accumulator.map.put(value, 1);
            }
        }
    }

    public void merge(HistogramAccumulator<T> accumulator, Iterable<HistogramAccumulator<T>> others)
            throws Exception {
        for (HistogramAccumulator<T> other : others) {
            for (Map.Entry<T, Integer> entry : other.map.entries()) {
                T key = entry.getKey();
                Integer newCount = entry.getValue();
                Integer oldCount = accumulator.map.get(key);
                if (accumulator.map.size() < LIMIT) {
                    if (oldCount == null) {
                        accumulator.map.put(key, newCount);
                    } else {
                        accumulator.map.put(key, oldCount + newCount);
                    }
                }
            }
        }
    }

    public void retract(HistogramAccumulator<T> accumulator, T value) throws Exception {
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

    @Override
    public MapData getValue(HistogramAccumulator<T> accumulator) {
        return new GenericMapData(accumulator.map.getMap());
    }
}
