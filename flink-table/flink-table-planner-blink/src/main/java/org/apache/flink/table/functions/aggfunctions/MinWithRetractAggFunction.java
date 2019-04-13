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
 * built-in Min with retraction aggregate function.
 */
public abstract class MinWithRetractAggFunction<T extends Comparable>
		extends AggregateFunction<T, MinWithRetractAggFunction.MinWithRetractAccumulator<T>> {

	/** The initial accumulator for Min with retraction aggregate function. */
	public static class MinWithRetractAccumulator<T> {
		public T min;
		public Long mapSize;
		public MapView<T, Long> map;
	}

	@Override
	public MinWithRetractAccumulator<T> createAccumulator() {
		MinWithRetractAccumulator<T> acc = new MinWithRetractAccumulator<>();
		acc.min = null; // min
		acc.mapSize = 0L;
		// store the count for each value
		acc.map = new MapView<>(getValueTypeInfo(), BasicTypeInfo.LONG_TYPE_INFO);
		return acc;
	}

	public void accumulate(MinWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			if (acc.mapSize == 0L || acc.min.compareTo(v) > 0) {
				acc.min = v;
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

	public void retract(MinWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			Long count = acc.map.get(v);
			if (count == null || count == 1L) {
				//remove the key v from the map if the number of appearance of the value v is 0
				if (count != null) {
					acc.map.remove(v);
				}
				//if the total count is 0, we could just simply set the f0(min) to the initial value
				acc.mapSize -= 1L;
				if (acc.mapSize <= 0L) {
					acc.min = null;
					return;
				}
				//if v is the current min value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the min value
				if (v == acc.min) {
					Iterator<T> iterator = acc.map.keys().iterator();
					boolean hasMin = false;
					while (iterator.hasNext()) {
						T key = iterator.next();
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
					}
				}
			} else {
				acc.map.put(v, count - 1);
			}
		}
	}

	public void merge(MinWithRetractAccumulator<T> acc, Iterable<MinWithRetractAccumulator<T>> its) throws Exception {
		Iterator<MinWithRetractAccumulator<T>> iter = its.iterator();
		while (iter.hasNext()) {
			MinWithRetractAccumulator<T> a = iter.next();
			if (a.mapSize != 0) {
				// set min element
				if (acc.mapSize == 0 || acc.min.compareTo(a.min) > 0) {
					acc.min = a.min;
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

	public void resetAccumulator(MinWithRetractAccumulator<T> acc) {
		acc.min = null;
		acc.mapSize = 0L;
		acc.map.clear();
	}

	@Override
	public T getValue(MinWithRetractAccumulator<T> acc) {
		if (acc.mapSize != 0) {
			return acc.min;
		} else {
			return null;
		}
	}

	protected abstract TypeInformation<?> getValueTypeInfo();

	/**
	 * Built-in Byte Min with retraction aggregate function.
	 */
	public static class ByteMinWithRetractAggFunction extends MinWithRetractAggFunction<Byte> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BYTE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Short Min with retraction aggregate function.
	 */
	public static class ShortMinWithRetractAggFunction extends MinWithRetractAggFunction<Short> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.SHORT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Int Min with retraction aggregate function.
	 */
	public static class IntMinWithRetractAggFunction extends MinWithRetractAggFunction<Integer> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.INT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Long Min with retraction aggregate function.
	 */
	public static class LongMinWithRetractAggFunction extends MinWithRetractAggFunction<Long> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}
	}

	/**
	 * Built-in Float Min with retraction aggregate function.
	 */
	public static class FloatMinWithRetractAggFunction extends MinWithRetractAggFunction<Float> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.FLOAT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Double Min with retraction aggregate function.
	 */
	public static class DoubleMinWithRetractAggFunction extends MinWithRetractAggFunction<Double> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Boolean Min with retraction aggregate function.
	 */
	public static class BooleanMinWithRetractAggFunction extends MinWithRetractAggFunction<Boolean> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}
	}

	/**
	 * Built-in Big Decimal Min with retraction aggregate function.
	 */
	public static class DecimalMinWithRetractAggFunction extends MinWithRetractAggFunction<Decimal> {
		private DecimalTypeInfo decimalType;

		public DecimalMinWithRetractAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in String Min with retraction aggregate function.
	 */
	public static class StringMinWithRetractAggFunction extends MinWithRetractAggFunction<BinaryString> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BinaryStringTypeInfo.INSTANCE;
		}
	}

	/**
	 * Built-in Timestamp Min with retraction aggregate function.
	 */
	public static class TimestampMinWithRetractAggFunction extends MinWithRetractAggFunction<Timestamp> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIMESTAMP;
		}
	}

	/**
	 * Built-in Date Min with retraction aggregate function.
	 */
	public static class DateMinWithRetractAggFunction extends MinWithRetractAggFunction<Date> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_DATE;
		}
	}

	/**
	 * Built-in Time Min with retraction aggregate function.
	 */
	public static class TimeMinWithRetractAggFunction extends MinWithRetractAggFunction<Time> {

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIME;
		}
	}
}
