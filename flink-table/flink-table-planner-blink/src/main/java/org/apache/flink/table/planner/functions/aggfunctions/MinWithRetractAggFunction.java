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
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.SqlTimestampTypeInfo;

import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * built-in Min with retraction aggregate function.
 */
public abstract class MinWithRetractAggFunction<T extends Comparable>
		extends AggregateFunction<T, MinWithRetractAggFunction.MinWithRetractAccumulator<T>> {

	private static final long serialVersionUID = 4253774292802374843L;

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

	public void retract(MinWithRetractAccumulator<T> acc, Object value) throws Exception {
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

				//if the total count is 0, we could just simply set the f0(min) to the initial value
				if (acc.mapSize == 0) {
					acc.min = null;
					return;
				}
				//if v is the current min value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the min value
				if (v.equals(acc.min)) {
					updateMin(acc);
				}
			} else {
				// store it when count is NOT zero
				acc.map.put(v, count);
				// we do not take negative number account into mapSize
			}
		}
	}

	private void updateMin(MinWithRetractAccumulator<T> acc) throws Exception {
		boolean hasMin = false;
		for (T key : acc.map.keys()) {
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
			// we should also override min value, because it may have an old value.
			acc.min = null;
		}
	}

	public void merge(MinWithRetractAccumulator<T> acc, Iterable<MinWithRetractAccumulator<T>> its) throws Exception {
		boolean needUpdateMin = false;
		for (MinWithRetractAccumulator<T> a : its) {
			// set min element
			if (acc.mapSize == 0 || (a.mapSize > 0 && a.min != null && acc.min.compareTo(a.min) > 0)) {
				acc.min = a.min;
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
						if (key.equals(acc.min)) {
							needUpdateMin = true;
						}
					}
				} else if (mergedCount < 0) {
					acc.map.put(key, mergedCount);
					if (thisCount > 0) {
						// origin is > 0, and retract to < 0
						acc.mapSize -= 1;
						if (key.equals(acc.min)) {
							needUpdateMin = true;
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
		if (needUpdateMin) {
			updateMin(acc);
		}
	}

	public void resetAccumulator(MinWithRetractAccumulator<T> acc) {
		acc.min = null;
		acc.mapSize = 0L;
		acc.map.clear();
	}

	@Override
	public T getValue(MinWithRetractAccumulator<T> acc) {
		if (acc.mapSize > 0) {
			return acc.min;
		} else {
			return null;
		}
	}

	@Override
	public TypeInformation<MinWithRetractAccumulator<T>> getAccumulatorType() {
		PojoTypeInfo pojoType = (PojoTypeInfo) TypeExtractor.createTypeInfo(MinWithRetractAccumulator.class);
		List<PojoField> pojoFields = new ArrayList<>();
		for (int i = 0; i < pojoType.getTotalFields(); i++) {
			PojoField field = pojoType.getPojoFieldAt(i);
			if (field.getField().getName().equals("min")) {
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
	 * Built-in Byte Min with retraction aggregate function.
	 */
	public static class ByteMinWithRetractAggFunction extends MinWithRetractAggFunction<Byte> {

		private static final long serialVersionUID = 3170462557144510063L;

		@Override
		protected TypeInformation<Byte> getValueTypeInfo() {
			return BasicTypeInfo.BYTE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Short Min with retraction aggregate function.
	 */
	public static class ShortMinWithRetractAggFunction extends MinWithRetractAggFunction<Short> {

		private static final long serialVersionUID = -4877567451203730974L;

		@Override
		protected TypeInformation<Short> getValueTypeInfo() {
			return BasicTypeInfo.SHORT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Int Min with retraction aggregate function.
	 */
	public static class IntMinWithRetractAggFunction extends MinWithRetractAggFunction<Integer> {

		private static final long serialVersionUID = -3187801696860321834L;

		@Override
		protected TypeInformation<Integer> getValueTypeInfo() {
			return BasicTypeInfo.INT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Long Min with retraction aggregate function.
	 */
	public static class LongMinWithRetractAggFunction extends MinWithRetractAggFunction<Long> {

		private static final long serialVersionUID = -3224670103852172282L;

		@Override
		protected TypeInformation<Long> getValueTypeInfo() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}
	}

	/**
	 * Built-in Float Min with retraction aggregate function.
	 */
	public static class FloatMinWithRetractAggFunction extends MinWithRetractAggFunction<Float> {

		private static final long serialVersionUID = 6683867851550125554L;

		@Override
		protected TypeInformation<Float> getValueTypeInfo() {
			return BasicTypeInfo.FLOAT_TYPE_INFO;
		}
	}

	/**
	 * Built-in Double Min with retraction aggregate function.
	 */
	public static class DoubleMinWithRetractAggFunction extends MinWithRetractAggFunction<Double> {

		private static final long serialVersionUID = -9107897474595423074L;

		@Override
		protected TypeInformation<Double> getValueTypeInfo() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
	}

	/**
	 * Built-in Boolean Min with retraction aggregate function.
	 */
	public static class BooleanMinWithRetractAggFunction extends MinWithRetractAggFunction<Boolean> {

		private static final long serialVersionUID = -4667566512148979776L;

		@Override
		protected TypeInformation<Boolean> getValueTypeInfo() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}
	}

	/**
	 * Built-in Big Decimal Min with retraction aggregate function.
	 */
	public static class DecimalMinWithRetractAggFunction extends MinWithRetractAggFunction<Decimal> {
		private static final long serialVersionUID = -7984016112363017960L;
		private DecimalTypeInfo decimalType;

		public DecimalMinWithRetractAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		public void accumulate(MinWithRetractAccumulator<Decimal> acc, Decimal value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MinWithRetractAccumulator<Decimal> acc, Decimal value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected TypeInformation<Decimal> getValueTypeInfo() {
			return decimalType;
		}
	}

	/**
	 * Built-in String Min with retraction aggregate function.
	 */
	public static class StringMinWithRetractAggFunction extends MinWithRetractAggFunction<BinaryString> {

		private static final long serialVersionUID = -6402993104400269468L;

		public void accumulate(MinWithRetractAccumulator<BinaryString> acc, BinaryString value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MinWithRetractAccumulator<BinaryString> acc, BinaryString value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected TypeInformation<BinaryString> getValueTypeInfo() {
			return BinaryStringTypeInfo.INSTANCE;
		}
	}

	/**
	 * Built-in Timestamp Min with retraction aggregate function.
	 */
	public static class TimestampMinWithRetractAggFunction extends MinWithRetractAggFunction<SqlTimestamp> {

		private static final long serialVersionUID = -7494198823345305907L;

		private final int precision;

		public TimestampMinWithRetractAggFunction(int precision) {
			this.precision = precision;
		}

		public void accumulate(MinWithRetractAccumulator<SqlTimestamp> acc, SqlTimestamp value) throws Exception {
			super.accumulate(acc, value);
		}

		public void retract(MinWithRetractAccumulator<SqlTimestamp> acc, SqlTimestamp value) throws Exception {
			super.retract(acc, value);
		}

		@Override
		protected TypeInformation<SqlTimestamp> getValueTypeInfo() {
			return new SqlTimestampTypeInfo(precision);
		}
	}

	/**
	 * Built-in Date Min with retraction aggregate function.
	 */
	public static class DateMinWithRetractAggFunction extends MinWithRetractAggFunction<Date> {

		private static final long serialVersionUID = 604406649989470870L;

		@Override
		protected TypeInformation<Date> getValueTypeInfo() {
			return Types.SQL_DATE;
		}
	}

	/**
	 * Built-in Time Min with retraction aggregate function.
	 */
	public static class TimeMinWithRetractAggFunction extends MinWithRetractAggFunction<Time> {

		private static final long serialVersionUID = -6908371577415696291L;

		@Override
		protected TypeInformation<Time> getValueTypeInfo() {
			return Types.SQL_TIME;
		}
	}
}
