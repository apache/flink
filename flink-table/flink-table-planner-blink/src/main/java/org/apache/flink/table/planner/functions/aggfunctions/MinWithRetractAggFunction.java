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
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in MIN with retraction aggregate function. */
@Internal
public final class MinWithRetractAggFunction<T extends Comparable<T>>
        extends BuiltInAggregateFunction<
                T, MinWithRetractAggFunction.MinWithRetractAccumulator<T>> {

    private static final long serialVersionUID = 4253774292802374843L;

    private transient DataType valueDataType;

    public MinWithRetractAggFunction(LogicalType valueType) {
        this.valueDataType = toInternalDataType(valueType);
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                MinWithRetractAccumulator.class,
                DataTypes.FIELD("min", valueDataType.nullable()),
                DataTypes.FIELD("mapSize", DataTypes.BIGINT()),
                DataTypes.FIELD(
                        "map",
                        MapView.newMapViewDataType(valueDataType.notNull(), DataTypes.BIGINT())));
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataType;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for MIN with retraction. */
    public static class MinWithRetractAccumulator<T> {
        public T min;
        public Long mapSize;
        public MapView<T, Long> map;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MinWithRetractAccumulator)) {
                return false;
            }
            MinWithRetractAccumulator<?> that = (MinWithRetractAccumulator<?>) o;
            return Objects.equals(min, that.min)
                    && Objects.equals(mapSize, that.mapSize)
                    && Objects.equals(map, that.map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(min, mapSize, map);
        }
    }

    @Override
    public MinWithRetractAccumulator<T> createAccumulator() {
        final MinWithRetractAccumulator<T> acc = new MinWithRetractAccumulator<>();
        acc.min = null;
        acc.mapSize = 0L;
        acc.map = new MapView<>();
        return acc;
    }

    public void accumulate(MinWithRetractAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            if (acc.mapSize == 0L || acc.min.compareTo(value) > 0) {
                acc.min = value;
            }

            Long count = acc.map.get(value);
            if (count == null) {
                count = 0L;
            }
            count += 1L;
            if (count == 0) {
                // remove it when count is increased from -1 to 0
                acc.map.remove(value);
            } else {
                // store it when count is NOT zero
                acc.map.put(value, count);
            }
            if (count == 1L) {
                // previous count is zero, this is the first time to see the key
                acc.mapSize += 1;
            }
        }
    }

    public void retract(MinWithRetractAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            Long count = acc.map.get(value);
            if (count == null) {
                count = 0L;
            }
            count -= 1;
            if (count == 0) {
                // remove it when count is decreased from 1 to 0
                acc.map.remove(value);
                acc.mapSize -= 1L;

                // if the total count is 0, we could just simply set the f0(min) to the initial
                // value
                if (acc.mapSize == 0) {
                    acc.min = null;
                    return;
                }
                // if v is the current min value, we have to iterate the map to find the 2nd biggest
                // value to replace v as the min value
                if (value.equals(acc.min)) {
                    updateMin(acc);
                }
            } else {
                // store it when count is NOT zero
                acc.map.put(value, count);
                // we do not take negative number account into mapSize
            }
        }
    }

    private void updateMin(MinWithRetractAccumulator<T> acc) throws Exception {
        boolean hasMin = false;
        for (T key : acc.map.keys()) {
            if (!hasMin || acc.min.compareTo(key) > 0) {
                acc.min = key;
                hasMin = true;
            }
        }
        // The behavior of deleting expired data in the state backend is uncertain.
        // so `mapSize` data may exist, while `map` data may have been deleted
        // when both of them are expired.
        if (!hasMin) {
            acc.mapSize = 0L;
            // we should also override min value, because it may have an old value.
            acc.min = null;
        }
    }

    public void merge(MinWithRetractAccumulator<T> acc, Iterable<MinWithRetractAccumulator<T>> its)
            throws Exception {
        boolean needUpdateMin = false;
        for (MinWithRetractAccumulator<T> a : its) {
            // set min element
            if (acc.mapSize == 0
                    || (a.mapSize > 0 && a.min != null && acc.min.compareTo(a.min) > 0)) {
                acc.min = a.min;
            }
            // merge the count for each key
            for (Map.Entry<T, Long> entry : a.map.entries()) {
                T key = entry.getKey();
                Long otherCount = entry.getValue(); // non-null
                Long thisCount = acc.map.get(key);
                if (thisCount == null) {
                    thisCount = 0L;
                }
                long mergedCount = otherCount + thisCount;
                if (mergedCount == 0) {
                    // remove it when count is increased from -1 to 0
                    acc.map.remove(key);
                    if (thisCount > 0) {
                        // origin is > 0, and retract to 0
                        acc.mapSize -= 1;
                        if (key.equals(acc.min)) {
                            needUpdateMin = true;
                        }
                    }
                } else if (mergedCount < 0) {
                    acc.map.put(key, mergedCount);
                    if (thisCount > 0) {
                        // origin is > 0, and retract to < 0
                        acc.mapSize -= 1;
                        if (key.equals(acc.min)) {
                            needUpdateMin = true;
                        }
                    }
                } else { // mergedCount > 0
                    acc.map.put(key, mergedCount);
                    if (thisCount <= 0) {
                        // origin is <= 0, and accumulate to > 0
                        acc.mapSize += 1;
                    }
                }
            }
        }
        if (needUpdateMin) {
            updateMin(acc);
        }
    }

    public void resetAccumulator(MinWithRetractAccumulator<T> acc) {
        acc.min = null;
        acc.mapSize = 0L;
        acc.map.clear();
    }

    @Override
    public T getValue(MinWithRetractAccumulator<T> acc) {
        if (acc.mapSize > 0) {
            return acc.min;
        } else {
            return null;
        }
    }
}
