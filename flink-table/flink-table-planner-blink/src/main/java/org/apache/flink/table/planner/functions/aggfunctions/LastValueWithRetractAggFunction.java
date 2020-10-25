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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/**
 * Built-in LAST_VALUE with retraction aggregate function.
 */
@Internal
public final class LastValueWithRetractAggFunction<T>
		extends InternalAggregateFunction<T, LastValueWithRetractAggFunction.LastValueWithRetractAccumulator<T>> {

	private transient DataType valueDataType;

	public LastValueWithRetractAggFunction(LogicalType valueType) {
		this.valueDataType = toInternalDataType(valueType);
	}

	// --------------------------------------------------------------------------------------------
	// Planning
	// --------------------------------------------------------------------------------------------

	@Override
	public DataType[] getInputDataTypes() {
		return new DataType[]{valueDataType};
	}

	@Override
	public DataType getAccumulatorDataType() {
		return DataTypes.STRUCTURED(
			LastValueWithRetractAccumulator.class,
			DataTypes.FIELD(
				"lastValue",
				valueDataType.nullable()),
			DataTypes.FIELD(
				"lastOrder",
				DataTypes.BIGINT()),
			DataTypes.FIELD(
				"valueToOrderMap",
				MapView.newMapViewDataType(
					valueDataType.notNull(),
					DataTypes.ARRAY(DataTypes.BIGINT()).bridgedTo(List.class))),
			DataTypes.FIELD(
				"orderToValueMap",
				MapView.newMapViewDataType(
					DataTypes.BIGINT(),
					DataTypes.ARRAY(valueDataType.notNull()).bridgedTo(List.class)))
		);
	}

	@Override
	public DataType getOutputDataType() {
		return valueDataType;
	}

	// --------------------------------------------------------------------------------------------
	// Runtime
	// --------------------------------------------------------------------------------------------

	/** Accumulator for LAST_VALUE with retraction. */
	public static class LastValueWithRetractAccumulator<T> {
		public T lastValue = null;
		public Long lastOrder = null;
		public MapView<T, List<Long>> valueToOrderMap = new MapView<>();
		public MapView<Long, List<T>> orderToValueMap = new MapView<>();

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof LastValueWithRetractAccumulator)) {
				return false;
			}
			LastValueWithRetractAccumulator<?> that = (LastValueWithRetractAccumulator<?>) o;
			return Objects.equals(lastValue, that.lastValue) &&
				Objects.equals(lastOrder, that.lastOrder) &&
				valueToOrderMap.equals(that.valueToOrderMap) &&
				orderToValueMap.equals(that.orderToValueMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(lastValue, lastOrder, valueToOrderMap, orderToValueMap);
		}
	}

	@Override
	public LastValueWithRetractAccumulator<T> createAccumulator() {
		return new LastValueWithRetractAccumulator<>();
	}

	@SuppressWarnings("unchecked")
	public void accumulate(LastValueWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long order = System.currentTimeMillis();
			List<Long> orderList = acc.valueToOrderMap.get(v);
			if (orderList == null) {
				orderList = new ArrayList<>();
			}
			orderList.add(order);
			acc.valueToOrderMap.put(v, orderList);
			accumulate(acc, value, order);
		}
	}

	@SuppressWarnings("unchecked")
	public void accumulate(LastValueWithRetractAccumulator<T> acc, Object value, Long order) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long prevOrder = acc.lastOrder;
			if (prevOrder == null || prevOrder <= order) {
				acc.lastValue = v;
				acc.lastOrder = order;
			}

			List<T> valueList = acc.orderToValueMap.get(order);
			if (valueList == null) {
				valueList = new ArrayList<>();
			}
			valueList.add(v);
			acc.orderToValueMap.put(order, valueList);
		}
	}

	public void accumulate(LastValueWithRetractAccumulator<T> acc, StringData value) throws Exception {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy());
		}
	}

	public void accumulate(LastValueWithRetractAccumulator<T> acc, StringData value, Long order) throws Exception {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy(), order);
		}
	}

	@SuppressWarnings("unchecked")
	public void retract(LastValueWithRetractAccumulator<T> acc, Object value) throws Exception {
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
				retract(acc, value, order);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void retract(LastValueWithRetractAccumulator<T> acc, Object value, Long order) throws Exception {
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
			if (v.equals(acc.lastValue)) {
				Long startKey = acc.lastOrder;
				Iterator<Long> iter = acc.orderToValueMap.keys().iterator();
				// find the maximal order which is less than or equal to `startKey`
				Long nextKey = Long.MIN_VALUE;
				while (iter.hasNext()) {
					Long key = iter.next();
					if (key <= startKey && key > nextKey) {
						nextKey = key;
					}
				}

				if (nextKey != Long.MIN_VALUE) {
					List<T> values = acc.orderToValueMap.get(nextKey);
					acc.lastValue = values.get(values.size() - 1);
					acc.lastOrder = nextKey;
				} else {
					acc.lastValue = null;
					acc.lastOrder = null;
				}
			}
		}
	}

	public void resetAccumulator(LastValueWithRetractAccumulator<T> acc) {
		acc.lastValue = null;
		acc.lastOrder = null;
		acc.valueToOrderMap.clear();
		acc.orderToValueMap.clear();
	}

	@Override
	public T getValue(LastValueWithRetractAccumulator<T> acc) {
		return acc.lastValue;
	}
}
