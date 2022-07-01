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
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Built-in FIRST_VALUE with retraction aggregate function. */
@Internal
public final class FirstValueWithRetractAggFunction<T>
        extends BuiltInAggregateFunction<
                T, FirstValueWithRetractAggFunction.FirstValueWithRetractAccumulator<T>> {

    private final transient DataType[] valueDataTypes;
    private final boolean ignoreNullByDefault;

    public FirstValueWithRetractAggFunction(LogicalType... valueTypes) {
        this(valueTypes, true);
    }

    public FirstValueWithRetractAggFunction(
            LogicalType[] valueTypes, boolean nullTreatmentByDefault) {
        this.valueDataTypes =
                Arrays.stream(valueTypes)
                        .map(DataTypeUtils::toInternalDataType)
                        .toArray(DataType[]::new);
        this.ignoreNullByDefault = !nullTreatmentByDefault;
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Arrays.asList(valueDataTypes);
    }

    @Override
    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                FirstValueWithRetractAccumulator.class,
                DataTypes.FIELD("firstValue", valueDataTypes[0].nullable()),
                DataTypes.FIELD("firstOrder", DataTypes.BIGINT()),
                DataTypes.FIELD(
                        "valueToOrderMap",
                        MapView.newMapViewDataType(
                                valueDataTypes[0],
                                DataTypes.ARRAY(DataTypes.BIGINT()).bridgedTo(List.class))),
                DataTypes.FIELD(
                        "orderToValueMap",
                        MapView.newMapViewDataType(
                                DataTypes.BIGINT(),
                                DataTypes.ARRAY(valueDataTypes[0]).bridgedTo(List.class))),
                DataTypes.FIELD(
                        "nullValueOrderList", ListView.newListViewDataType(DataTypes.BIGINT())));
    }

    @Override
    public DataType getOutputDataType() {
        return valueDataTypes[0];
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
        /**
         * the separate order list for null value since {@link ListView} does not allow {@code
         * null}s .
         */
        public ListView<Long> nullValueOrderList;

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
                    && Objects.equals(orderToValueMap, that.orderToValueMap)
                    && Objects.equals(nullValueOrderList, that.nullValueOrderList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    firstValue, firstOrder, valueToOrderMap, orderToValueMap, nullValueOrderList);
        }
    }

    @Override
    public FirstValueWithRetractAccumulator<T> createAccumulator() {
        final FirstValueWithRetractAccumulator<T> acc = new FirstValueWithRetractAccumulator<>();
        acc.firstValue = null;
        acc.firstOrder = null;
        acc.valueToOrderMap = new MapView<>();
        acc.orderToValueMap = new MapView<>();
        acc.nullValueOrderList = new ListView<>();
        return acc;
    }

    public void accumulate(FirstValueWithRetractAccumulator<T> acc, Object value) throws Exception {
        accumulate(acc, value, ignoreNullByDefault);
    }

    @SuppressWarnings("unchecked")
    public void accumulate(
            FirstValueWithRetractAccumulator<T> acc, Object value, boolean ignoreNull)
            throws Exception {
        // only when the value isn't null or not to ignore null, accumulate it
        if (value != null || !ignoreNull) {
            T v = (T) value;
            Long order = System.currentTimeMillis();
            List<Long> orderList = getValueOrderList(acc, v);
            if (orderList == null) {
                orderList = new ArrayList<>();
            }
            orderList.add(order);
            putValueOrderList(acc, v, orderList);
            accumulate(acc, v, order, ignoreNull);
        }
    }

    public void accumulate(FirstValueWithRetractAccumulator<T> acc, Object value, Long order)
            throws Exception {
        accumulate(acc, value, order, ignoreNullByDefault);
    }

    @SuppressWarnings("unchecked")
    public void accumulate(
            FirstValueWithRetractAccumulator<T> acc, Object value, Long order, boolean ignoreNull)
            throws Exception {
        // only when the value isn't null or not to ignore null, accumulate it
        if ((value != null || !ignoreNull) && order != null) {
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
        accumulate(acc, value, ignoreNullByDefault);
    }

    public void accumulate(
            FirstValueWithRetractAccumulator<T> acc, StringData value, boolean ignoreNull)
            throws Exception {
        // when value isn't null, accumulate it
        if (value != null) {
            accumulate(acc, (Object) ((BinaryStringData) value).copy());
        } else if (!ignoreNull) {
            // otherwise, if not ignore null, accumulate it
            accumulate(acc, (Object) null, false);
        }
    }

    public void accumulate(FirstValueWithRetractAccumulator<T> acc, StringData value, Long order)
            throws Exception {
        accumulate(acc, value, order, ignoreNullByDefault);
    }

    public void accumulate(
            FirstValueWithRetractAccumulator<T> acc,
            StringData value,
            Long order,
            boolean ignoreNull)
            throws Exception {
        // when value isn't null, accumulate it
        if (value != null) {
            accumulate(acc, (Object) ((BinaryStringData) value).copy(), order);
        } else if (!ignoreNull) {
            // otherwise, if not ignore null, accumulate it
            accumulate(acc, (Object) null, order, false);
        }
    }

    public void retract(FirstValueWithRetractAccumulator<T> acc, Object value) throws Exception {
        retract(acc, value, ignoreNullByDefault);
    }

    @SuppressWarnings("unchecked")
    public void retract(FirstValueWithRetractAccumulator<T> acc, Object value, boolean ignoreNull)
            throws Exception {
        // only when the value isn't null or not to ignore null, retract it
        if (value != null || !ignoreNull) {
            T v = (T) value;
            List<Long> orderList = getValueOrderList(acc, v);
            if (orderList != null && orderList.size() > 0) {
                Long order = orderList.get(0);
                orderList.remove(0);
                if (orderList.isEmpty()) {
                    removeValueFromValueToOrderMap(acc, v);
                } else {
                    putValueOrderList(acc, v, orderList);
                }
                retract(acc, v, order, ignoreNull);
            }
        }
    }

    public void retract(FirstValueWithRetractAccumulator<T> acc, Object value, Long order)
            throws Exception {
        retract(acc, value, order, ignoreNullByDefault);
    }

    @SuppressWarnings("unchecked")
    public void retract(
            FirstValueWithRetractAccumulator<T> acc, Object value, Long order, boolean ignoreNull)
            throws Exception {
        // only when the value isn't null or not to ignore null, retract it
        if ((value != null || !ignoreNull) && order != null) {
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
            if ((v == null && acc.firstValue == null) || (v != null && v.equals(acc.firstValue))) {
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

    private List<Long> getValueOrderList(FirstValueWithRetractAccumulator<T> acc, T value)
            throws Exception {
        if (value == null) {
            return acc.nullValueOrderList.getList();
        } else {
            return acc.valueToOrderMap.get(value);
        }
    }

    public void putValueOrderList(
            FirstValueWithRetractAccumulator<T> acc, T value, List<Long> orderList)
            throws Exception {
        if (value == null) {
            acc.nullValueOrderList.setList(orderList);
        } else {
            acc.valueToOrderMap.put(value, orderList);
        }
    }

    public void removeValueFromValueToOrderMap(FirstValueWithRetractAccumulator<T> acc, T value)
            throws Exception {
        if (value == null) {
            acc.nullValueOrderList.clear();
        } else {
            acc.valueToOrderMap.remove(value);
        }
    }

    public void resetAccumulator(FirstValueWithRetractAccumulator<T> acc) {
        acc.firstValue = null;
        acc.firstOrder = null;
        acc.valueToOrderMap.clear();
        acc.orderToValueMap.clear();
        acc.nullValueOrderList.clear();
    }

    @Override
    public T getValue(FirstValueWithRetractAccumulator<T> acc) {
        return acc.firstValue;
    }
}
