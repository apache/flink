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

/** Built-in MAX with retraction aggregate function. */
@Internal
public final class MaxWithRetractAggFunction<T extends Comparable<T>>
        extends BuiltInAggregateFunction<
                T, MaxWithRetractAggFunction.MaxWithRetractAccumulator<T>> {

    private static final long serialVersionUID = -5860934997657147836L;

    private transient DataType valueDataType;

    public MaxWithRetractAggFunction(LogicalType valueType) {
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
                MaxWithRetractAccumulator.class,
                DataTypes.FIELD("max", valueDataType.nullable()),
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

    /** Accumulator for MAX with retraction. */
    public static class MaxWithRetractAccumulator<T> {
        public T max;
        public Long mapSize;
        public MapView<T, Long> map;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MaxWithRetractAccumulator)) {
                return false;
            }
            MaxWithRetractAccumulator<?> that = (MaxWithRetractAccumulator<?>) o;
            return Objects.equals(max, that.max)
                    && Objects.equals(mapSize, that.mapSize)
                    && Objects.equals(map, that.map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(max, mapSize, map);
        }
    }

    @Override
    public MaxWithRetractAccumulator<T> createAccumulator() {
        final MaxWithRetractAccumulator<T> acc = new MaxWithRetractAccumulator<>();
        acc.max = null;
        acc.mapSize = 0L;
        acc.map = new MapView<>();
        return acc;
    }

    public void accumulate(MaxWithRetractAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            if (acc.mapSize == 0L || acc.max.compareTo(value) < 0) {
                acc.max = value;
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

    public void retract(MaxWithRetractAccumulator<T> acc, T value) throws Exception {
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

                // if the total count is 0, we could just simply set the f0(max) to the initial
                // value
                if (acc.mapSize == 0) {
                    acc.max = null;
                    return;
                }
                // if v is the current max value, we have to iterate the map to find the 2nd biggest
                // value to replace v as the max value
                if (value.equals(acc.max)) {
                    updateMax(acc);
                }
            } else {
                // store it when count is NOT zero
                acc.map.put(value, count);
                // we do not take negative number account into mapSize
            }
        }
    }

    private void updateMax(MaxWithRetractAccumulator<T> acc) throws Exception {
        boolean hasMax = false;
        for (T key : acc.map.keys()) {
            if (!hasMax || acc.max.compareTo(key) < 0) {
                acc.max = key;
                hasMax = true;
            }
        }
        // The behavior of deleting expired data in the state backend is uncertain.
        // so `mapSize` data may exist, while `map` data may have been deleted
        // when both of them are expired.
        if (!hasMax) {
            acc.mapSize = 0L;
            // we should also override max value, because it may have an old value.
            acc.max = null;
        }
    }

    public void merge(MaxWithRetractAccumulator<T> acc, Iterable<MaxWithRetractAccumulator<T>> its)
            throws Exception {
        boolean needUpdateMax = false;
        for (MaxWithRetractAccumulator<T> a : its) {
            // set max element
            if (acc.mapSize == 0
                    || (a.mapSize > 0 && a.max != null && acc.max.compareTo(a.max) < 0)) {
                acc.max = a.max;
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
                        if (key.equals(acc.max)) {
                            needUpdateMax = true;
                        }
                    }
                } else if (mergedCount < 0) {
                    acc.map.put(key, mergedCount);
                    if (thisCount > 0) {
                        // origin is > 0, and retract to < 0
                        acc.mapSize -= 1;
                        if (key.equals(acc.max)) {
                            needUpdateMax = true;
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
        if (needUpdateMax) {
            updateMax(acc);
        }
    }

    public void resetAccumulator(MaxWithRetractAccumulator<T> acc) {
        acc.max = null;
        acc.mapSize = 0L;
        acc.map.clear();
    }

    @Override
    public T getValue(MaxWithRetractAccumulator<T> acc) {
        if (acc.mapSize > 0) {
            return acc.max;
        } else {
            return null;
        }
    }
}
