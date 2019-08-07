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
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.hll.HyperLogLogPlusPlus;
import org.apache.flink.table.planner.functions.aggfunctions.hll.XxHash64Function;
import org.apache.flink.table.types.logical.DecimalType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * built-in approx_count_distinct aggregate function.
 */
public abstract class ApproximateCountDistinctAggFunction<T>
		extends AggregateFunction<Long, ApproximateCountDistinctAggFunction.HllAccumulator> {

	private static final long serialVersionUID = 2373428076681873676L;
	private static final Double RELATIVE_SD = 0.01;
	private HyperLogLogPlusPlus hyperLogLogPlusPlus;

	/**
	 * Accumulator used by HLL++.
	 */
	public static class HllAccumulator {
		public long[] array;
	}

	abstract long getHashcode(T t);

	@Override
	public HllAccumulator createAccumulator() {
		hyperLogLogPlusPlus = new HyperLogLogPlusPlus(RELATIVE_SD);
		HllAccumulator accumulator = new HllAccumulator();
		accumulator.array = new long[hyperLogLogPlusPlus.getNumWords()];
		resetAccumulator(accumulator);
		return accumulator;
	}

	public void accumulate(HllAccumulator accumulator, Object input) throws Exception {
		if (input != null) {
			hyperLogLogPlusPlus.updateByHashcode(accumulator, getHashcode((T) input));
		}
	}

	public void merge(HllAccumulator accumulator, Iterable<HllAccumulator> it) throws Exception {
		for (HllAccumulator tmpAcc : it) {
			hyperLogLogPlusPlus.merge(accumulator, tmpAcc);
		}
	}

	public void resetAccumulator(HllAccumulator accumulator) {
		int word = 0;
		while (word < hyperLogLogPlusPlus.getNumWords()) {
			accumulator.array[word] = 0;
			word++;
		}
	}

	@Override
	public Long getValue(HllAccumulator accumulator) {
		return hyperLogLogPlusPlus.query(accumulator);
	}

	@Override
	public TypeInformation<HllAccumulator> getAccumulatorType() {
		return new GenericTypeInfo<>(HllAccumulator.class);
	}

	/**
	 * Built-in byte approximate count distinct aggregate function.
	 */
	public static class ByteApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Byte> {

		@Override
		long getHashcode(Byte value) {
			return XxHash64Function.hashInt(value, XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in decimal approximate count distinct aggregate function.
	 */
	public static class DecimalApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Decimal> {

		private final int precision;
		private final int scale;

		public DecimalApproximateCountDistinctAggFunction(DecimalType decimalType) {
			this.precision = decimalType.getPrecision();
			this.scale = decimalType.getScale();
		}

		public void accumulate(HllAccumulator accumulator, Decimal input) throws Exception {
			super.accumulate(accumulator, input);
		}

		@Override
		long getHashcode(Decimal d) {
			if (precision <= Decimal.MAX_LONG_DIGITS) {
				return XxHash64Function.hashLong(d.toUnscaledLong(), XxHash64Function.DEFAULT_SEED);
			} else {
				byte[] bytes = d.toBigDecimal().unscaledValue().toByteArray();
				MemorySegment segment = MemorySegmentFactory.wrap(bytes);
				return XxHash64Function.hashUnsafeBytes(segment, 0, segment.size(), XxHash64Function.DEFAULT_SEED);
			}
		}
	}

	/**
	 * Built-in double approximate count distinct aggregate function.
	 */
	public static class DoubleApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Double> {

		@Override
		long getHashcode(Double value) {
			return XxHash64Function.hashLong(Double.doubleToLongBits(value), XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in float approximate count distinct aggregate function.
	 */
	public static class FloatApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Float> {

		@Override
		long getHashcode(Float value) {
			return XxHash64Function.hashInt(Float.floatToIntBits(value), XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in int approximate count distinct aggregate function.
	 */
	public static class IntApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Integer> {

		@Override
		long getHashcode(Integer value) {
			return XxHash64Function.hashInt(value, XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in long approximate count distinct aggregate function.
	 */
	public static class LongApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Long> {

		@Override
		long getHashcode(Long value) {
			return XxHash64Function.hashLong(value, XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in short approximate count distinct aggregate function.
	 */
	public static class ShortApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Short> {

		@Override
		long getHashcode(Short value) {
			return XxHash64Function.hashInt(value, XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in boolean approximate count distinct aggregate function.
	 */
	public static class BooleanApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Boolean> {

		@Override
		long getHashcode(Boolean value) {
			return XxHash64Function.hashInt(value ? 1 : 0, XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in date approximate count distinct aggregate function.
	 */
	public static class DateApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Date> {

		public void accumulate(HllAccumulator accumulator, Date input) throws Exception {
			super.accumulate(accumulator, input);
		}

		@Override
		long getHashcode(Date value) {
			return XxHash64Function.hashLong(value.getTime(), XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in time approximate count distinct aggregate function.
	 */
	public static class TimeApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Time> {

		public void accumulate(HllAccumulator accumulator, Time input) throws Exception {
			super.accumulate(accumulator, input);
		}

		@Override
		long getHashcode(Time value) {
			return XxHash64Function.hashLong(value.getTime(), XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in timestamp approximate count distinct aggregate function.
	 */
	public static class TimestampApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<Timestamp> {

		public void accumulate(HllAccumulator accumulator, Timestamp input) throws Exception {
			super.accumulate(accumulator, input);
		}

		@Override
		long getHashcode(Timestamp value) {
			return XxHash64Function.hashLong(value.getTime(), XxHash64Function.DEFAULT_SEED);
		}
	}

	/**
	 * Built-in string approximate count distinct aggregate function.
	 */
	public static class StringApproximateCountDistinctAggFunction
			extends ApproximateCountDistinctAggFunction<BinaryString> {

		public void accumulate(HllAccumulator accumulator, BinaryString input) throws Exception {
			if (input != null) {
				super.accumulate(accumulator, input.copy());
			}
		}

		@Override
		long getHashcode(BinaryString s) {
			MemorySegment[] segments = s.getSegments();
			if (segments.length == 1) {
				return XxHash64Function.hashUnsafeBytes(
						segments[0], s.getOffset(), s.getSizeInBytes(), XxHash64Function.DEFAULT_SEED);
			} else {
				return XxHash64Function.hashUnsafeBytes(
						MemorySegmentFactory.wrap(s.getBytes()), 0, s.getSizeInBytes(), XxHash64Function.DEFAULT_SEED);
			}
		}
	}

}
