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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in FIRST_VALUE with retraction aggregate function. */
@Internal
public final class FirstValueWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<T>> {

    private transient DataType valueDataType;

    public FirstValueWithRetractAggFunction(LogicalType valueType) {
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
                FirstValueWithRetractAccumulator.class,
                DataTypes.FIELD("firstValue", valueDataType.nullable()),
                DataTypes.FIELD("firstOrder", DataTypes.BIGINT()),
                DataTypes.FIELD(
                        "valueToOrderMap",
                        MapView.newMapViewDataType(
                                valueDataType.notNull(),
                                DataTypes.ARRAY(DataTypes.BIGINT()).bridgedTo(List.class))),
                DataTypes.FIELD(
                        "orderToValueMap",
                        MapView.newMapViewDataType(
                                DataTypes.BIGINT(),
                                DataTypes.ARRAY(valueDataType.notNull()).bridgedTo(List.class))));
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataType;
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for FIRST_VALUE. */
    public static class FirstValueWithRetractAccumulator<T> {
        public T firstValue;
        public Long firstOrder;
        public MapView<T, List<Long>> valueToOrderMap;
        public MapView<Long, List<T>> orderToValueMap;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FirstValueWithRetractAccumulator)) {
                return false;
            }
            FirstValueWithRetractAccumulator<?> that = (FirstValueWithRetractAccumulator<?>) o;
            return Objects.equals(firstValue, that.firstValue)
                    && Objects.equals(firstOrder, that.firstOrder)
                    && Objects.equals(valueToOrderMap, that.valueToOrderMap)
                    && Objects.equals(orderToValueMap, that.orderToValueMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstValue, firstOrder, valueToOrderMap, orderToValueMap);
        }
    }

    @Override
    public FirstValueWithRetractAccumulator<T> createAccumulator() {
        final FirstValueWithRetractAccumulator<T> acc = new FirstValueWithRetractAccumulator<>();
        acc.firstValue = null;
        acc.firstOrder = null;
        acc.valueToOrderMap = new MapView<>();
        acc.orderToValueMap = new MapView<>();
        return acc;
    }

    @SuppressWarnings("unchecked")
    public void accumulate(FirstValueWithRetractAccumulator<T> acc, Object value) throws Exception {
        if (value != null) {
            T v = (T) value;
            Long order = System.currentTimeMillis();
            List<Long> orderList = acc.valueToOrderMap.get(v);
            if (orderList == null) {
                orderList = new ArrayList<>();
            }
            orderList.add(order);
            acc.valueToOrderMap.put(v, orderList);
            accumulate(acc, v, order);
        }
    }

    @SuppressWarnings("unchecked")
    public void accumulate(FirstValueWithRetractAccumulator<T> acc, Object value, Long order)
            throws Exception {
        if (value != null) {
            T v = (T) value;
            Long prevOrder = acc.firstOrder;
            if (prevOrder == null || prevOrder > order) {
                acc.firstValue = v;
                acc.firstOrder = order;
            }

            List<T> valueList = acc.orderToValueMap.get(order);
            if (valueList == null) {
                valueList = new ArrayList<>();
            }
            valueList.add(v);
            acc.orderToValueMap.put(order, valueList);
        }
    }

    public void accumulate(FirstValueWithRetractAccumulator<T> acc, StringData value)
            throws Exception {
        if (value != null) {
            accumulate(acc, (Object) ((BinaryStringData) value).copy());
        }
    }

    public void accumulate(FirstValueWithRetractAccumulator<T> acc, StringData value, Long order)
            throws Exception {
        if (value != null) {
            accumulate(acc, (Object) ((BinaryStringData) value).copy(), order);
        }
    }

    @SuppressWarnings("unchecked")
    public void retract(FirstValueWithRetractAccumulator<T> acc, Object value) throws Exception {
        if (value != null) {
            T v = (T) value;
            List<Long> orderList = acc.valueToOrderMap.get(v);
            if (orderList != null && orderList.size() > 0) {
                Long order = orderList.get(0);
                orderList.remove(0);
                if (orderList.isEmpty()) {
                    acc.valueToOrderMap.remove(v);
                } else {
                    acc.valueToOrderMap.put(v, orderList);
                }
                retract(acc, v, order);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void retract(FirstValueWithRetractAccumulator<T> acc, Object value, Long order)
            throws Exception {
        if (value != null) {
            T v = (T) value;
            List<T> valueList = acc.orderToValueMap.get(order);
            if (valueList == null) {
                return;
            }
            int index = valueList.indexOf(v);
            if (index >= 0) {
                valueList.remove(index);
                if (valueList.isEmpty()) {
                    acc.orderToValueMap.remove(order);
                } else {
                    acc.orderToValueMap.put(order, valueList);
                }
            }
            if (v.equals(acc.firstValue)) {
                Long startKey = acc.firstOrder;
                Iterator<Long> iter = acc.orderToValueMap.keys().iterator();
                // find the minimal order which is greater than or equal to `startKey`
                Long nextKey = Long.MAX_VALUE;
                while (iter.hasNext()) {
                    Long key = iter.next();
                    if (key >= startKey && key < nextKey) {
                        nextKey = key;
                    }
                }

                if (nextKey != Long.MAX_VALUE) {
                    acc.firstValue = acc.orderToValueMap.get(nextKey).get(0);
                    acc.firstOrder = nextKey;
                } else {
                    acc.firstValue = null;
                    acc.firstOrder = null;
                }
            }
        }
    }

    public void resetAccumulator(FirstValueWithRetractAccumulator<T> acc) {
        acc.firstValue = null;
        acc.firstOrder = null;
        acc.valueToOrderMap.clear();
        acc.orderToValueMap.clear();
    }

    @Override
    public T getValue(FirstValueWithRetractAccumulator<T> acc) {
        return acc.firstValue;
    }
}
