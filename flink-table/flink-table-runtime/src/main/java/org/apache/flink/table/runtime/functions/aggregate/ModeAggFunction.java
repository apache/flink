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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in MODE with retraction aggregate function. */
@Internal
public class ModeAggFunction<T> extends BuiltInAggregateFunction<T, ModeAggFunction.ModeAcc<T>> {

    private final transient DataType valueDataType;

    public ModeAggFunction(LogicalType valueType) {
        this.valueDataType = toInternalDataType(valueType);
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(valueDataType);
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataType;
    }

    @Override
    public T getValue(ModeAcc<T> accumulator) {
        return accumulator.curMode;
    }

    @Override
    public ModeAcc<T> createAccumulator() {
        final ModeAcc<T> acc = new ModeAcc<>();
        acc.buffer = new MapView<>();
        acc.curCnt = 0;
        acc.curMode = null;
        return acc;
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                ModeAcc.class,
                DataTypes.FIELD("curCnt", DataTypes.BIGINT()),
                DataTypes.FIELD("curMode", valueDataType.nullable()),
                DataTypes.FIELD(
                        "buffer",
                        MapView.newMapViewDataType(valueDataType.notNull(), DataTypes.BIGINT())));
    }

    /** Accumulator for MODE. */
    public static class ModeAcc<T> {
        public long curCnt;
        public T curMode;
        public MapView<T, Long> buffer;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ModeAcc<?> lagAcc = (ModeAcc<?>) o;
            return curCnt == lagAcc.curCnt
                    && Objects.equals(curMode, lagAcc.curMode)
                    && Objects.equals(buffer, lagAcc.buffer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(curCnt, curMode, buffer);
        }
    }

    public void accumulate(ModeAcc<T> acc, T value) throws Exception {
        if (value == null) {
            return;
        }
        Long cnt = acc.buffer.get(value);
        cnt = cnt == null ? 1 : cnt + 1;
        acc.buffer.put(value, cnt);
        if (Objects.equals(value, acc.curMode)) {
            acc.curCnt = cnt;
        } else if (cnt > acc.curCnt) {
            acc.curMode = value;
            acc.curCnt = cnt;
        }
    }

    public void retract(ModeAcc<T> acc, T value) throws Exception {
        if (value == null) {
            return;
        }
        Long cnt = acc.buffer.get(value);
        cnt = cnt == null ? 0L : cnt - 1;
        if (cnt <= 0) {
            acc.buffer.remove(value);
        } else {
            acc.buffer.put(value, cnt);
        }
        acc.curCnt = cnt;
        if (Objects.equals(value, acc.curMode)) {
            for (Map.Entry<T, Long> entry : acc.buffer.entries()) {
                if (entry.getValue() > acc.curCnt) {
                    acc.curMode = entry.getKey();
                    acc.curCnt = entry.getValue();
                }
            }
        }
        if (acc.curCnt == 0) {
            acc.curMode = null;
        }
    }

    public void resetAccumulator(ModeAcc<T> acc) {
        acc.buffer.clear();
        acc.curCnt = 0;
        acc.curMode = null;
    }

    public void merge(ModeAggFunction.ModeAcc<T> acc, Iterable<ModeAggFunction.ModeAcc<T>> its)
            throws Exception {
        for (ModeAggFunction.ModeAcc<T> otherAcc : its) {
            if (!otherAcc.buffer.iterator().hasNext()) {
                // otherAcc is empty, skip it
                continue;
            }
            for (Map.Entry<T, Long> entry : otherAcc.buffer.entries()) {
                final T key = entry.getKey();
                final long newValue;
                if (acc.buffer.contains(key)) {
                    newValue = acc.buffer.get(key) + entry.getValue();
                    acc.buffer.put(key, newValue);
                } else {
                    newValue = entry.getValue();
                    acc.buffer.put(key, newValue);
                }
                if (newValue > acc.curCnt) {
                    acc.curMode = key;
                    acc.curCnt = newValue;
                }
            }
        }
    }
}
