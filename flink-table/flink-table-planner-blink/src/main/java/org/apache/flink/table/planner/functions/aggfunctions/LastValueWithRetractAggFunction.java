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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.dataformat.BinaryGeneric;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataview.MapViewSerializer;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryStringSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalSerializer;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TypeInformationRawType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;

/**
 * built-in LastValue with retraction aggregate function.
 */
public abstract class LastValueWithRetractAggFunction<T> extends AggregateFunction<T, GenericRow> {

	@Override
	public GenericRow createAccumulator() {
		// The accumulator schema:
		// lastValue: T
		// lastOrder: Long
		// valueToOrderMap: BinaryGeneric<MapView<T, List<Long>>>
		// orderToValueMap: BinaryGeneric<MapView<Long, List<T>>>
		GenericRow acc = new GenericRow(4);
		acc.setField(0, null);
		acc.setField(1, null);
		acc.setField(2, new BinaryGeneric<>(
				new MapView<>(getResultType(), new ListTypeInfo<>(Types.LONG))));
		acc.setField(3, new BinaryGeneric<>(
				new MapView<>(Types.LONG, new ListTypeInfo<>(getResultType()))));
		return acc;
	}

	public void accumulate(GenericRow acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long order = System.currentTimeMillis();
			MapView<T, List<Long>> valueToOrderMapView = getValueToOrderMapViewFromAcc(acc);
			List<Long> orderList = valueToOrderMapView.get(v);
			if (orderList == null) {
				orderList = new ArrayList<>();
			}
			orderList.add(order);
			valueToOrderMapView.put(v, orderList);
			accumulate(acc, value, order);
		}
	}

	public void accumulate(GenericRow acc, Object value, Long order) throws Exception {
		if (value != null) {
			T v = (T) value;
			Long prevOrder = (Long) acc.getField(1);
			if (prevOrder == null || prevOrder <= order) {
				acc.setField(0, v); // acc.lastValue = v
				acc.setLong(1, order); // acc.lastOrder = order
			}

			MapView<Long, List<T>> orderToValueMapView = getOrderToValueMapViewFromAcc(acc);
			List<T> valueList = orderToValueMapView.get(order);
			if (valueList == null) {
				valueList = new ArrayList<>();
			}
			valueList.add(v);
			orderToValueMapView.put(order, valueList);
		}
	}

	public void retract(GenericRow acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;
			MapView<T, List<Long>> valueToOrderMapView = getValueToOrderMapViewFromAcc(acc);
			List<Long> orderList = valueToOrderMapView.get(v);
			if (orderList != null && orderList.size() > 0) {
				Long order = orderList.get(0);
				orderList.remove(0);
				if (orderList.isEmpty()) {
					valueToOrderMapView.remove(v);
				} else {
					valueToOrderMapView.put(v, orderList);
				}
				retract(acc, value, order);
			}
		}
	}

	public void retract(GenericRow acc, Object value, Long order) throws Exception {
		if (value != null) {
			T v = (T) value;
			MapView<Long, List<T>> orderToValueMapView = getOrderToValueMapViewFromAcc(acc);
			List<T> valueList = orderToValueMapView.get(order);
			if (valueList == null) {
				return;
			}
			int index = valueList.indexOf(v);
			if (index >= 0) {
				valueList.remove(index);
				if (valueList.isEmpty()) {
					orderToValueMapView.remove(order);
				} else {
					orderToValueMapView.put(order, valueList);
				}
			}
			if (v.equals(acc.getField(0))) { // v == acc.firstValue
				Long startKey = (Long) acc.getField(1);
				Iterator<Long> iter = orderToValueMapView.keys().iterator();
				// find the maximal order which is less than or equal to `startKey`
				Long nextKey = Long.MIN_VALUE;
				while (iter.hasNext()) {
					Long key = iter.next();
					if (key <= startKey && key > nextKey) {
						nextKey = key;
					}
				}

				if (nextKey != Long.MIN_VALUE) {
					List<T> values = orderToValueMapView.get(nextKey);
					acc.setField(0, values.get(values.size() - 1));
					acc.setField(1, nextKey);
				} else {
					acc.setField(0, null);
					acc.setField(1, null);
				}
			}
		}
	}

	public void resetAccumulator(GenericRow acc) {
		acc.setField(0, null);
		acc.setField(1, null);
		MapView<T, List<Long>> valueToOrderMapView = getValueToOrderMapViewFromAcc(acc);
		valueToOrderMapView.clear();
		MapView<Long, List<T>> orderToValueMapView = getOrderToValueMapViewFromAcc(acc);
		orderToValueMapView.clear();
	}

	@Override
	public T getValue(GenericRow acc) {
		return (T) acc.getField(0);
	}

	protected abstract TypeSerializer<T> createValueSerializer();

	@Override
	public TypeInformation<GenericRow> getAccumulatorType() {
		LogicalType[] fieldTypes = new LogicalType[] {
				fromTypeInfoToLogicalType(getResultType()),
				new BigIntType(),
				new TypeInformationRawType<>(new MapViewTypeInfo<>(getResultType(), new ListTypeInfo<>(Types.LONG), false, false)),
				new TypeInformationRawType<>(new MapViewTypeInfo<>(Types.LONG, new ListTypeInfo<>(getResultType()), false, false))
		};

		String[] fieldNames = new String[] {
				"lastValue",
				"lastOrder",
				"valueToOrderMapView",
				"orderToValueMapView"
		};

		return (TypeInformation) new BaseRowTypeInfo(fieldTypes, fieldNames);
	}

	@SuppressWarnings("unchecked")
	private MapView<T, List<Long>> getValueToOrderMapViewFromAcc(GenericRow acc) {
		BinaryGeneric<MapView<T, List<Long>>> binaryGeneric =
				(BinaryGeneric<MapView<T, List<Long>>>) acc.getField(2);
		return BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric, getValueToOrderMapViewSerializer());
	}

	@SuppressWarnings("unchecked")
	private MapView<Long, List<T>> getOrderToValueMapViewFromAcc(GenericRow acc) {
		BinaryGeneric<MapView<Long, List<T>>> binaryGeneric =
				(BinaryGeneric<MapView<Long, List<T>>>) acc.getField(3);
		return BinaryGeneric.getJavaObjectFromBinaryGeneric(binaryGeneric, getOrderToValueMapViewSerializer());
	}

	// MapView<T, List<Long>>
	private MapViewSerializer<T, List<Long>> getValueToOrderMapViewSerializer() {
		return new MapViewSerializer<>(
				new MapSerializer<>(
						createValueSerializer(),
						new ListSerializer<>(LongSerializer.INSTANCE)));
	}

	// MapView<Long, List<T>>
	private MapViewSerializer<Long, List<T>> getOrderToValueMapViewSerializer() {
		return new MapViewSerializer<>(
				new MapSerializer<>(
						LongSerializer.INSTANCE,
						new ListSerializer<>(createValueSerializer())));
	}

	/**
	 * Built-in Byte LastValue with retract aggregate function.
	 */
	public static class ByteLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Byte> {

		@Override
		public TypeInformation<Byte> getResultType() {
			return Types.BYTE;
		}

		@Override
		protected TypeSerializer<Byte> createValueSerializer() {
			return ByteSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Short LastValue with retract aggregate function.
	 */
	public static class ShortLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Short> {

		@Override
		public TypeInformation<Short> getResultType() {
			return Types.SHORT;
		}

		@Override
		protected TypeSerializer<Short> createValueSerializer() {
			return ShortSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Int LastValue with retract aggregate function.
	 */
	public static class IntLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Integer> {

		@Override
		public TypeInformation<Integer> getResultType() {
			return Types.INT;
		}

		@Override
		protected TypeSerializer<Integer> createValueSerializer() {
			return IntSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Long LastValue with retract aggregate function.
	 */
	public static class LongLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Long> {

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}

		@Override
		protected TypeSerializer<Long> createValueSerializer() {
			return LongSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Float LastValue with retract aggregate function.
	 */
	public static class FloatLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Float> {

		@Override
		public TypeInformation<Float> getResultType() {
			return Types.FLOAT;
		}

		@Override
		protected TypeSerializer<Float> createValueSerializer() {
			return FloatSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Double LastValue with retract aggregate function.
	 */
	public static class DoubleLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Double> {

		@Override
		public TypeInformation<Double> getResultType() {
			return Types.DOUBLE;
		}

		@Override
		protected TypeSerializer<Double> createValueSerializer() {
			return DoubleSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Boolean LastValue with retract aggregate function.
	 */
	public static class BooleanLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Boolean> {

		@Override
		public TypeInformation<Boolean> getResultType() {
			return Types.BOOLEAN;
		}

		@Override
		protected TypeSerializer<Boolean> createValueSerializer() {
			return BooleanSerializer.INSTANCE;
		}
	}

	/**
	 * Built-in Decimal LastValue with retract aggregate function.
	 */
	public static class DecimalLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<Decimal> {

		private DecimalTypeInfo decimalTypeInfo;

		public DecimalLastValueWithRetractAggFunction(DecimalTypeInfo decimalTypeInfo) {
			this.decimalTypeInfo = decimalTypeInfo;
		}

		public void accumulate(GenericRow acc, Decimal value) throws Exception {
			super.accumulate(acc, value);
		}

		public void accumulate(GenericRow acc, Decimal value, Long order) throws Exception {
			super.accumulate(acc, value, order);
		}

		@Override
		public TypeInformation<Decimal> getResultType() {
			return decimalTypeInfo;
		}

		@Override
		protected TypeSerializer<Decimal> createValueSerializer() {
			return new DecimalSerializer(decimalTypeInfo.precision(), decimalTypeInfo.scale());
		}
	}

	/**
	 * Built-in String LastValue with retract aggregate function.
	 */
	public static class StringLastValueWithRetractAggFunction extends LastValueWithRetractAggFunction<BinaryString> {

		@Override
		public TypeInformation<BinaryString> getResultType() {
			return BinaryStringTypeInfo.INSTANCE;
		}

		public void accumulate(GenericRow acc, BinaryString value) throws Exception {
			if (value != null) {
				super.accumulate(acc, value.copy());
			}
		}

		public void accumulate(GenericRow acc, BinaryString value, Long order) throws Exception {
			// just ignore nulls values and orders
			if (value != null) {
				super.accumulate(acc, value.copy(), order);
			}
		}

		@Override
		protected TypeSerializer<BinaryString> createValueSerializer() {
			return BinaryStringSerializer.INSTANCE;
		}
	}
}
