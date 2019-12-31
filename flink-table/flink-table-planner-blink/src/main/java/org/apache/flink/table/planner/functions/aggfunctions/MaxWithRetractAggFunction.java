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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * built-in Max with retraction aggregate function.
 */
public abstract class MaxWithRetractAggFunction<T extends Comparable>
		extends AggregateFunction<T, MaxWithRetractAggFunction.MaxWithRetractAccumulator<T>> {

	private static final long serialVersionUID = -5860934997657147836L;

	/** The initial accumulator for Max with retraction aggregate function. */
	public static class MaxWithRetractAccumulator<T> {
		public T max;
		public Long mapSize;
		public MapView<T, Long> map;
	}

	@Override
	public MaxWithRetractAccumulator<T> createAccumulator() {
		MaxWithRetractAccumulator<T> acc = new MaxWithRetractAccumulator<>();
		acc.max = null; // max
		acc.mapSize = 0L;
		// store the count for each value
		acc.map = new MapView<>(getValueTypeInfo(), BasicTypeInfo.LONG_TYPE_INFO);
		return acc;
	}

	public void accumulate(MaxWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			if (acc.mapSize == 0L || acc.max.compareTo(v) < 0) {
				acc.max = v;
			}

			Long count = acc.map.get(v);
			if (count == null) {
				count = 0L;
			}
			count += 1L;
			if (count == 0) {
				// remove it when count is increased from -1 to 0
				acc.map.remove(v);
			} else {
				// store it when count is NOT zero
				acc.map.put(v, count);
			}
			if (count == 1L) {
				// previous count is zero, this is the first time to see the key
				acc.mapSize += 1;
			}
		}
	}

	public void retract(MaxWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			Long count = acc.map.get(v);
			if (count == null) {
				count = 0L;
			}
			count -= 1;
			if (count == 0) {
				// remove it when count is decreased from 1 to 0
				acc.map.remove(v);
				acc.mapSize -= 1L;

				//if the total count is 0, we could just simply set the f0(max) to the initial value
				if (acc.mapSize == 0) {
					acc.max = null;
					return;
				}
				//if v is the current max value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the max value
				if (v.equals(acc.max)) {
					updateMax(acc);
				}
			} else {
				// store it when count is NOT zero
				acc.map.put(v, count);
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

	public void merge(MaxWithRetractAccumulator<T> acc, Iterable<MaxWithRetractAccumulator<T>> its) throws Exception {
		boolean needUpdateMax = false;
		for (MaxWithRetractAccumulator<T> a : its) {
			// set max element
			if (acc.mapSize == 0 || (a.mapSize > 0 && a.max != null && acc.max.compareTo(a.max) < 0)) {
				acc.max = a.max;
			}
			// merge the count for each key
			for (Map.Entry entry : a.map.entries()) {
				T key = (T) entry.getKey();
				Long otherCount = (Long) entry.getValue(); // non-null
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

	@Override
	public TypeInformation<MaxWithRetractAccumulator<T>> getAccumulatorType() {
		PojoTypeInfo pojoType = (PojoTypeInfo) TypeExtractor.createTypeInfo(MaxWithRetractAccumulator.class);
		List<PojoField> pojoFields = new ArrayList<>();
		for (int i = 0; i < pojoType.getTotalFields(); i++) {
			PojoField field = pojoType.getPojoFieldAt(i);
			if (field.getField().getName().equals("max")) {
				pojoFields.add(new PojoField(field.getField(), getValueTypeInfo()));
			} else {
				pojoFields.add(field);
			}
		}
		//noinspection unchecked
		return new PojoTypeInfo(pojoType.getTypeClass(), pojoFields);
	}

	@Override
	public TypeInformation<T> getResultType() {
		return getValueTypeInfo();
	}

	protected abstract TypeInformation<T> getValueTypeInfo();

	/**
	 * Built-in Byte Max with retraction aggregate function.
	 */
	public static class ByteMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Byte> {

		private static final long serialVersionUID = 7383980948808353819L;

		@Override
		protected TypeInformation<Byte> getValueTypeInfo() {
			return BasicTypeInfo.BYTE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Short Max with retraction aggregate function.
	 */
	public static class ShortMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Short> {

		private static final long serialVersionUID = 7579072678911328694L;

		@Override
		protected TypeInformation<Short> getValueTypeInfo() {
			return BasicTypeInfo.SHORT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Int Max with retraction aggregate function.
	 */
	public static class IntMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		private static final long serialVersionUID = 3833976566544263072L;

		@Override
		protected TypeInformation<Integer> getValueTypeInfo() {
			return BasicTypeInfo.INT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Long Max with retraction aggregate function.
	 */
	public static class LongMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Long> {

		private static final long serialVersionUID = 8585384188523017375L;

		@Override
		protected TypeInformation<Long> getValueTypeInfo() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}
	}

	/**
	 * Built-in Float Max with retraction aggregate function.
	 */
	public static class FloatMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Float> {

		private static final long serialVersionUID = -1433882434794024584L;

		@Override
		protected TypeInformation<Float> getValueTypeInfo() {
			return BasicTypeInfo.FLOAT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Double Max with retraction aggregate function.
	 */
	public static class DoubleMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Double> {

		private static final long serialVersionUID = -1525221057708740308L;

		@Override
		protected TypeInformation<Double> getValueTypeInfo() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Boolean Max with retraction aggregate function.
	 */
	public static class BooleanMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Boolean> {

		private static final long serialVersionUID = -8408715018822625309L;

		@Override
		protected TypeInformation<Boolean> getValueTypeInfo() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}
	}

	/**
	 * Built-in Big Decimal Max with retraction aggregate function.
	 */
	public static class DecimalMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Decimal> {
		private static final long serialVersionUID = 5301860581297042635L;
		private DecimalTypeInfo decimalType;

		public DecimalMaxWithRetractAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		public void accumulate(MaxWithRetractAccumulator<Decimal> acc, Decimal value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MaxWithRetractAccumulator<Decimal> acc, Decimal value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected TypeInformation<Decimal> getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in String Max with retraction aggregate function.
	 */
	public static class StringMaxWithRetractAggFunction extends MaxWithRetractAggFunction<BinaryString> {

		private static final long serialVersionUID = 787528574867514796L;

		public void accumulate(MaxWithRetractAccumulator<BinaryString> acc, BinaryString value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MaxWithRetractAccumulator<BinaryString> acc, BinaryString value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected TypeInformation<BinaryString> getValueTypeInfo() {
			return BinaryStringTypeInfo.INSTANCE;
		}
	}

	/**
	 * Built-in Timestamp Max with retraction aggregate function.
	 */
	public static class TimestampMaxWithRetractAggFunction extends MaxWithRetractAggFunction<LocalDateTime> {

		private static final long serialVersionUID = -7096481949093142944L;

		@Override
		protected TypeInformation<LocalDateTime> getValueTypeInfo() {
			return Types.LOCAL_DATE_TIME;
		}
	}

	/**
	 * Built-in Date Max with retraction aggregate function.
	 */
	public static class DateMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Date> {

		private static final long serialVersionUID = 7452698503075473023L;

		@Override
		protected TypeInformation<Date> getValueTypeInfo() {
			return Types.SQL_DATE;
		}
	}

	/**
	 * Built-in Time Max with retraction aggregate function.
	 */
	public static class TimeMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Time> {

		private static final long serialVersionUID = 3578216747876121493L;

		@Override
		protected TypeInformation<Time> getValueTypeInfo() {
			return Types.SQL_TIME;
		}
	}
}
