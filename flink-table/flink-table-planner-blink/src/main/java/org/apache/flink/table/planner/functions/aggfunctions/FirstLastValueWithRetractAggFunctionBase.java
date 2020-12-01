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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/**
 * Base retraction aggregate function for FIRST_VALUE and LAST_VALUE.
 */
@Internal
abstract class FirstLastValueWithRetractAggFunctionBase<T>
		extends InternalAggregateFunction<T, FirstLastValueWithRetractAggFunctionBase.FirstLastValueWithRetractAccumulator<T>> {

	private transient DataType valueDataType;

	public FirstLastValueWithRetractAggFunctionBase(LogicalType valueType) {
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
			FirstLastValueWithRetractAccumulator.class,
			DataTypes.FIELD(
				"value",
				valueDataType.nullable()),
			DataTypes.FIELD(
				"order",
				DataTypes.BIGINT()),
			DataTypes.FIELD(
				"valueToOrderMap",
				MapView.newMapViewDataType(
					valueDataType.notNull(),
					DataTypes.ARRAY(DataTypes.BIGINT()).bridgedTo(List.class))),
			DataTypes.FIELD(
				"valueToOrderRetractMap",
				MapView.newMapViewDataType(
					valueDataType.notNull(),
					DataTypes.ARRAY(DataTypes.BIGINT()).bridgedTo(List.class))),
			DataTypes.FIELD(
				"orderToValueMap",
				MapView.newMapViewDataType(
					DataTypes.BIGINT(),
					DataTypes.ARRAY(valueDataType.notNull()).bridgedTo(List.class))),
			DataTypes.FIELD(
				"orderToValueRetractMap",
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

	/** Accumulator for FIRST_VALUE or LAST_VALUE. */
	public static class FirstLastValueWithRetractAccumulator<T> {
		public T value = null;
		public Long order = null;
		public MapView<T, List<Long>> valueToOrderMap = new MapView<>();
		public MapView<T, List<Long>> valueToOrderRetractMap = new MapView<>();
		public MapView<Long, List<T>> orderToValueMap = new MapView<>();
		public MapView<Long, List<T>> orderToValueRetractMap = new MapView<>();

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof FirstLastValueWithRetractAccumulator)) {
				return false;
			}
			FirstLastValueWithRetractAccumulator<?> that = (FirstLastValueWithRetractAccumulator<?>) o;
			return Objects.equals(value, that.value) &&
				Objects.equals(order, that.order) &&
				Objects.equals(valueToOrderMap, that.valueToOrderMap) &&
				Objects.equals(valueToOrderRetractMap, that.valueToOrderRetractMap) &&
				Objects.equals(orderToValueMap, that.orderToValueMap) &&
				Objects.equals(orderToValueRetractMap, that.orderToValueRetractMap);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				value,
				order,
				valueToOrderMap,
				valueToOrderRetractMap,
				orderToValueMap,
				orderToValueRetractMap);
		}
	}

	@Override
	public FirstLastValueWithRetractAccumulator<T> createAccumulator() {
		return new FirstLastValueWithRetractAccumulator<>();
	}

	@SuppressWarnings("unchecked")
	public void accumulate(
			FirstLastValueWithRetractAccumulator<T> acc,
			Object value,
			BiFunction<Long, Long, Boolean> compare) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long order = System.nanoTime();
			accumulate(acc, v, order, compare);
		}
	}

	@SuppressWarnings("unchecked")
	public void accumulate(
			FirstLastValueWithRetractAccumulator<T> acc,
			Object value, Long order,
			BiFunction<Long, Long, Boolean> compare) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long prevOrder = acc.order;
			if (prevOrder == null || compare.apply(prevOrder, order)) {
				acc.value = v;
				acc.order = order;
			}

			// save order to orderToValueMap
			List<Long> orderList = acc.valueToOrderMap.get(v);
			if (orderList == null) {
				orderList = new ArrayList<>();
				orderList.add(order);
			} else {
				// use insertion sort,
				// for the O(1) time complexity if order is asc, compare from back to front
				int i = orderList.size();
				boolean isInsert = false;
				while (i > 0) {
					Long tempOrder = orderList.get(i - 1);
					if (tempOrder <= order) {
						orderList.add(i, order);
						isInsert = true;
						break;
					}
					i--;
				}
				if (!isInsert) {
					orderList.add(0, order);
				}
			}
			// update valueToOrderMap
			acc.valueToOrderMap.put(v, orderList);

			// update orderToValueMap
			List<T> valueList = acc.orderToValueMap.get(order);
			if (valueList == null) {
				valueList = new ArrayList<>();
			}
			valueList.add(v);
			acc.orderToValueMap.put(order, valueList);
		}
	}

	public void accumulate(FirstLastValueWithRetractAccumulator<T> acc, StringData value, BiFunction<Long, Long, Boolean> compare) throws Exception {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy(), compare);
		}
	}

	public void accumulate(FirstLastValueWithRetractAccumulator<T> acc, StringData value, Long order, BiFunction<Long, Long, Boolean> compare) throws Exception {
		if (value != null) {
			accumulate(acc, (Object) ((BinaryStringData) value).copy(), order, compare);
		}
	}

	@SuppressWarnings("unchecked")
	public void retract(FirstLastValueWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;
			List<Long> orderList = acc.valueToOrderMap.get(v);
			if (orderList != null && orderList.size() > 0) {
				Long order = orderList.get(0);
				retract(acc, v, order);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void retract(FirstLastValueWithRetractAccumulator<T> acc, Object value, Long order) throws Exception {
		if (value != null) {
			T v = (T) value;
			// remove from valueToOrderMap
			List<Long> orderList = acc.valueToOrderMap.get(v);
			if (orderList != null && orderList.size() > 0) {
				orderList.remove(0);
				if (orderList.isEmpty()) {
					acc.valueToOrderMap.remove(v);
				} else {
					acc.valueToOrderMap.put(v, orderList);
				}
			} else {
				// if not exist, save to valueToOrderRetractMap
				List<Long> retractOrderList = acc.valueToOrderRetractMap.get(v);
				if (retractOrderList == null) {
					retractOrderList = new ArrayList<>();
				}
				retractOrderList.add(order);
				acc.valueToOrderRetractMap.put(v, retractOrderList);
			}
			// remove from orderToValueMap
			List<T> valueList = acc.orderToValueMap.get(order);
			int index = -1;
			if (valueList != null &&  (index = valueList.indexOf(v)) >= 0) {
				valueList.remove(index);
				if (valueList.isEmpty()) {
					acc.orderToValueMap.remove(order);
				} else {
					acc.orderToValueMap.put(order, valueList);
				}
			} else {
				// if not exist, save to orderToValueRetractMap
				List<T> retractValueList = acc.orderToValueRetractMap.get(order);
				if (retractValueList == null) {
					retractValueList = new ArrayList<>();
				}
				retractValueList.add(v);
				acc.orderToValueRetractMap.put(order, retractValueList);
			}
			// if retract current first value
			if (v.equals(acc.value)) {
				updateValue(acc);
			}
		}
	}

	public void merge(
			FirstLastValueWithRetractAccumulator<T> acc,
			Iterable<FirstLastValueWithRetractAccumulator<T>> its,
			BiFunction<Long, Long, Boolean> compare) throws Exception {
		for (FirstLastValueWithRetractAccumulator<T> it : its) {
			if (acc.order == null || (it.order != null && compare.apply(acc.order, it.order))) {
				acc.value = it.value;
				acc.order = it.order;
			}
			mergeValueToOrderMap(acc, it);
			mergeOrderToValueMap(acc, it);
			// if current first value is retract
			if (!acc.valueToOrderMap.contains(acc.value)) {
				updateValue(acc);
			}
		}
	}

	public void resetAccumulator(FirstLastValueWithRetractAccumulator<T> acc) {
		acc.value = null;
		acc.order = null;
		acc.valueToOrderMap.clear();
		acc.valueToOrderRetractMap.clear();
		acc.orderToValueMap.clear();
		acc.orderToValueRetractMap.clear();
	}

	@Override
	public T getValue(FirstLastValueWithRetractAccumulator<T> acc) {
		return acc.value;
	}

	private void mergeOrderToValueMap(
			FirstLastValueWithRetractAccumulator<T> acc,
			FirstLastValueWithRetractAccumulator<T> it) throws Exception {
		// merge orderToValueMap
		for (Map.Entry<Long, List<T>> entry : it.orderToValueMap.entries()) {
			Long key = entry.getKey();
			if (acc.orderToValueMap.contains(key)) {
				List<T> itList = entry.getValue();
				List<T> accList = acc.orderToValueMap.get(key);
				// merge
				List<T> mergedList = mergeOrderedList(itList, accList, new Comparator<T>() {
					@Override
					public int compare(T o1, T o2) {
						try {
							Long o1MinOrder = acc.valueToOrderMap.get(o1).get(0);
							Long o2MinOrder = acc.valueToOrderMap.get(o2).get(0);
							return (int) (o1MinOrder - o2MinOrder);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				});
				acc.orderToValueMap.put(key, mergedList);
			} else {
				// if order doesn't exist, put to orderToValueMap
				acc.orderToValueMap.put(key, entry.getValue());
			}
		}

		// merge orderToValueRetractMap
		for (Map.Entry<Long, List<T>> entry : it.orderToValueRetractMap.entries()) {
			Long key = entry.getKey();
			if (acc.orderToValueRetractMap.contains(key)) {
				List<T> valueList = acc.orderToValueRetractMap.get(key);
				valueList.addAll(entry.getValue());
				acc.orderToValueRetractMap.put(key, valueList);
			} else {
				acc.orderToValueRetractMap.put(key, entry.getValue());
			}
		}

		// retract from orderToValueRetractMap
		Iterator<Map.Entry<Long, List<T>>> orderToValueRetractMapIt =
			acc.orderToValueRetractMap.entries().iterator();
		while (orderToValueRetractMapIt.hasNext()) {
			Map.Entry<Long, List<T>> entry = orderToValueRetractMapIt.next();
			Long key = entry.getKey();
			if (acc.orderToValueMap.contains(key)) {
				List<T> valueList = acc.orderToValueMap.get(key);
				List<T> retractValueList = entry.getValue();
				List<T> newRetractValueList = new ArrayList<>();
				for (int i = 0; i < retractValueList.size(); i++) {
					if (!valueList.remove(retractValueList.get(i))) {
						newRetractValueList.add(retractValueList.get(i));
					}
				}
				if (valueList.size() > 0) {
					acc.orderToValueMap.put(key, valueList);
				} else {
					acc.orderToValueMap.remove(key);
				}
				if (newRetractValueList.size() > 0) {
					acc.orderToValueRetractMap.put(key, newRetractValueList);
				} else {
					orderToValueRetractMapIt.remove();
				}
			}
		}
	}

	private void mergeValueToOrderMap(
			FirstLastValueWithRetractAccumulator<T> acc,
			FirstLastValueWithRetractAccumulator<T> it) throws Exception {
		// merge valueToOrderMap
		for (Map.Entry<T, List<Long>> entry : it.valueToOrderMap.entries()) {
			T key = entry.getKey();
			if (acc.valueToOrderMap.contains(key)) {
				List<Long> itList = entry.getValue();
				List<Long> accList = acc.valueToOrderMap.get(key);
				// merge
				List<Long> mergedList = mergeOrderedList(itList, accList, new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return (int) (o1 - o2);
					}
				});
				acc.valueToOrderMap.put(key, mergedList);
			} else {
				// if value doesn't exist, put to valueToOrderMap
				acc.valueToOrderMap.put(key, entry.getValue());
			}
		}

		// merge valueToOrderRetractMap
		for (Map.Entry<T, List<Long>> entry : it.valueToOrderRetractMap.entries()) {
			T key = entry.getKey();
			if (acc.valueToOrderRetractMap.contains(key)) {
				List<Long> accList = acc.valueToOrderMap.get(key);
				accList.addAll(entry.getValue());
				acc.valueToOrderRetractMap.put(key, accList);
			} else {
				acc.valueToOrderRetractMap.put(key, entry.getValue());
			}
		}

		// retract from valueToOrderRetractMap
		Iterator<Map.Entry<T, List<Long>>> valueToOrderRetractMapIt =
			acc.valueToOrderRetractMap.entries().iterator();
		while (valueToOrderRetractMapIt.hasNext()) {
			Map.Entry<T, List<Long>> entry = valueToOrderRetractMapIt.next();
			T key = entry.getKey();
			if (acc.valueToOrderMap.contains(key)) {
				List<Long> orderList = acc.valueToOrderMap.get(key);
				List<Long> retractOrderList = entry.getValue();
				List<Long> newRetractOrderList = new ArrayList<>();
				for (int i = 0; i < retractOrderList.size(); i++) {
					if (!orderList.remove(retractOrderList.get(i))) {
						newRetractOrderList.add(retractOrderList.get(i));
					}
				}
				if (orderList.size() > 0) {
					acc.valueToOrderMap.put(key, orderList);
				} else {
					acc.valueToOrderMap.remove(key);
				}
				if (newRetractOrderList.size() > 0) {
					acc.valueToOrderRetractMap.put(key, newRetractOrderList);
				} else {
					valueToOrderRetractMapIt.remove();
				}
			}
		}

	}

	/**
	 *  Merge two lists which are ordered, and the time complexity is O(n+m).
	 * @param list1 first ordered list
	 * @param list2 second ordered list
	 * @param comparator comparator used for sort
	 * @return ordered list after merging list1 and list2
	 */
	private List mergeOrderedList(List list1, List list2, Comparator comparator) {
		ArrayList mergedList = new ArrayList<>();
		int i = 0;
		int j = 0;
		while (i < list1.size() && j < list2.size()) {
			Object value1 = list1.get(i);
			Object value2 = list2.get(j);
			if (comparator.compare(value1, value2) < 0) {
				mergedList.add(i + j, value1);
				i++;
			} else {
				mergedList.add(i + j, value2);
				j++;
			}
		}
		while (i < list1.size()) {
			mergedList.add(i + j, list1.get(i));
			i++;
		}
		while (j < list2.size()) {
			mergedList.add(i + j, list2.get(j));
			j++;
		}
		return mergedList;
	}

	/**
	 *  Update the first or last value if it is retracted.
	 */
	abstract void updateValue(FirstLastValueWithRetractAccumulator<T> acc) throws Exception;
}
