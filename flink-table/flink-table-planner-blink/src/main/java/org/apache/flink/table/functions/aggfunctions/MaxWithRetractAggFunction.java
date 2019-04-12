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

package org.apache.flink.table.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * built-in Max with retraction aggregate function.
 */
public abstract class MaxWithRetractAggFunction<T extends Comparable>
		extends AggregateFunction<T, MaxWithRetractAggFunction.MaxWithRetractAccumulator<T>> {

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
				acc.map.put(v, 1L);
				acc.mapSize += 1;
			} else {
				count += 1L;
				acc.map.put(v, count);
			}
		}
	}

	public void retract(MaxWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			Long count = acc.map.get(v);
			if (count == null || count == 1L) {
				//remove the key v from the map if the number of appearance of the value v is 0
				if (count != null) {
					acc.map.remove(v);
				}
				//if the total count is 0, we could just simply set the f0(max) to the initial value
				acc.mapSize -= 1L;
				if (acc.mapSize <= 0L) {
					acc.max = null;
					return;
				}
				//if v is the current max value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the max value
				if (v == acc.max) {
					Iterator<T> iterator = acc.map.keys().iterator();
					boolean hasMax = false;
					while (iterator.hasNext()) {
						T key = iterator.next();
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
					}
				}
			} else {
				acc.map.put(v, count - 1);
			}
		}
	}

	public void merge(MaxWithRetractAccumulator<T> acc, Iterable<MaxWithRetractAccumulator<T>> its) throws Exception {
		Iterator<MaxWithRetractAccumulator<T>> iter = its.iterator();
		while (iter.hasNext()) {
			MaxWithRetractAccumulator<T> a = iter.next();
			if (a.mapSize != 0) {
				// set max element
				if (acc.mapSize == 0 || acc.max.compareTo(a.max) < 0) {
					acc.max = a.max;
				}
				// merge the count for each key
				Iterator<Map.Entry<T, Long>> iterator = a.map.entries().iterator();
				while (iterator.hasNext()) {
					Map.Entry entry = iterator.next();
					T key = (T) entry.getKey();
					Long value = (Long) entry.getValue();
					Long count = acc.map.get(key);
					if (count != null) {
						acc.map.put(key, count + value);
					} else {
						acc.map.put(key, value);
						acc.mapSize += 1;
					}
				}
			}
		}
	}

	public void resetAccumulator(MaxWithRetractAccumulator<T> acc) {
		acc.max = null;
		acc.mapSize = 0L;
		acc.map.clear();
	}

	@Override
	public T getValue(MaxWithRetractAccumulator<T> acc) {
		if (acc.mapSize != 0) {
			return acc.max;
		} else {
			return null;
		}
	}

	protected abstract TypeInformation<?> getValueTypeInfo();

	/**
	 * Built-in Byte Max with retraction aggregate function.
	 */
	public static class ByteMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Byte> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BYTE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Short Max with retraction aggregate function.
	 */
	public static class ShortMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Short> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.SHORT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Int Max with retraction aggregate function.
	 */
	public static class IntMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.INT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Long Max with retraction aggregate function.
	 */
	public static class LongMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Long> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}
	}

	/**
	 * Built-in Float Max with retraction aggregate function.
	 */
	public static class FloatMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Float> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.FLOAT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Double Max with retraction aggregate function.
	 */
	public static class DoubleMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Double> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Boolean Max with retraction aggregate function.
	 */
	public static class BooleanMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Boolean> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}
	}

	/**
	 * Built-in Big Decimal Max with retraction aggregate function.
	 */
	public static class DecimalMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Decimal> {
		private DecimalTypeInfo decimalType;

		public DecimalMaxWithRetractAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in String Max with retraction aggregate function.
	 */
	public static class StringMaxWithRetractAggFunction extends MaxWithRetractAggFunction<BinaryString> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BinaryStringTypeInfo.INSTANCE;
		}
	}

	/**
	 * Built-in Timestamp Max with retraction aggregate function.
	 */
	public static class TimestampMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Timestamp> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIMESTAMP;
		}
	}

	/**
	 * Built-in Date Max with retraction aggregate function.
	 */
	public static class DateMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Date> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_DATE;
		}
	}

	/**
	 * Built-in Time Max with retraction aggregate function.
	 */
	public static class TimeMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Time> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIME;
		}
	}
}
