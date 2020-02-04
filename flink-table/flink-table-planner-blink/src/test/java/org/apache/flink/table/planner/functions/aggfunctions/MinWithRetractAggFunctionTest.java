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

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.BooleanMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.ByteMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.DateMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.DecimalMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.DoubleMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.FloatMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.IntMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.LongMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.MinWithRetractAccumulator;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.ShortMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.StringMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.TimeMinWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinWithRetractAggFunction.TimestampMinWithRetractAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in Min with retraction aggregate function.
 */
public abstract class MinWithRetractAggFunctionTest<T> extends AggFunctionTestBase<T, MinWithRetractAccumulator<T>> {

	@Override
	protected Class<?> getAccClass() {
		return MinWithRetractAccumulator.class;
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
	}

	/**
	 * Test MinWithRetractAggFunction for number type.
	 */
	public abstract static class NumberMinWithRetractAggFunctionTest<T> extends MinWithRetractAggFunctionTest<T> {
		protected abstract T getMinValue();

		protected abstract T getMaxValue();

		protected abstract T getValue(String v);

		@Override
		protected List<List<T>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							getValue("1"),
							null,
							getMaxValue(),
							getValue("-99"),
							getValue("3"),
							getValue("56"),
							getValue("0"),
							getMinValue(),
							getValue("-20"),
							getValue("17"),
							null
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							getValue("10")
					)
			);
		}

		@Override
		protected List<T> getExpectedResults() {
			return Arrays.asList(
					getMinValue(),
					null,
					getValue("10")
			);
		}
	}

	/**
	 * Test for ByteMinWithRetractAggFunction.
	 */
	public static class ByteMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Byte> {

		@Override
		protected Byte getMinValue() {
			return Byte.MIN_VALUE + 1;
		}

		@Override
		protected Byte getMaxValue() {
			return Byte.MAX_VALUE - 1;
		}

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, MinWithRetractAccumulator<Byte>> getAggregator() {
			return new ByteMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for ShortMinWithRetractAggFunction.
	 */
	public static class ShortMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Short> {

		@Override
		protected Short getMinValue() {
			return Short.MIN_VALUE + 1;
		}

		@Override
		protected Short getMaxValue() {
			return Short.MAX_VALUE - 1;
		}

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, MinWithRetractAccumulator<Short>> getAggregator() {
			return new ShortMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for IntMinWithRetractAggFunction.
	 */
	public static class IntMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Integer> {

		@Override
		protected Integer getMinValue() {
			return Integer.MIN_VALUE + 1;
		}

		@Override
		protected Integer getMaxValue() {
			return Integer.MAX_VALUE - 1;
		}

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, MinWithRetractAccumulator<Integer>> getAggregator() {
			return new IntMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for LongMinWithRetractAggFunction.
	 */
	public static class LongMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Long> {

		@Override
		protected Long getMinValue() {
			return Long.MIN_VALUE + 1;
		}

		@Override
		protected Long getMaxValue() {
			return Long.MAX_VALUE - 1;
		}

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, MinWithRetractAccumulator<Long>> getAggregator() {
			return new LongMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for FloatMinWithRetractAggFunction.
	 */
	public static class FloatMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Float> {

		@Override
		protected Float getMinValue() {
			return -Float.MAX_VALUE / 2;
		}

		@Override
		protected Float getMaxValue() {
			return Float.MAX_VALUE / 2;
		}

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, MinWithRetractAccumulator<Float>> getAggregator() {
			return new FloatMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleMinWithRetractAggFunction.
	 */
	public static class DoubleMinWithRetractAggFunctionTest extends NumberMinWithRetractAggFunctionTest<Double> {

		@Override
		protected Double getMinValue() {
			return -Double.MAX_VALUE / 2;
		}

		@Override
		protected Double getMaxValue() {
			return Double.MAX_VALUE / 2;
		}

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, MinWithRetractAccumulator<Double>> getAggregator() {
			return new DoubleMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanMinWithRetractAggFunction.
	 */
	public static class BooleanMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest<Boolean> {

		@Override
		protected List<List<Boolean>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							false,
							false,
							false
					),
					Arrays.asList(
							true,
							true,
							true
					),
					Arrays.asList(
							true,
							false,
							null,
							true,
							false,
							true,
							null
					),
					Arrays.asList(
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							true
					));
		}

		@Override
		protected List<Boolean> getExpectedResults() {
			return Arrays.asList(
					false,
					true,
					false,
					null,
					true
			);
		}

		@Override
		protected AggregateFunction<Boolean, MinWithRetractAccumulator<Boolean>> getAggregator() {
			return new BooleanMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalMinWithRetractAggFunction.
	 */
	public static class DecimalMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest<Decimal> {

		private int precision = 20;
		private int scale = 6;

		@Override
		protected List<List<Decimal>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							Decimal.castFrom("1", precision, scale),
							Decimal.castFrom("1000", precision, scale),
							Decimal.castFrom("-1", precision, scale),
							Decimal.castFrom("-999.998999", precision, scale),
							null,
							Decimal.castFrom("0", precision, scale),
							Decimal.castFrom("-999.999", precision, scale),
							null,
							Decimal.castFrom("999.999", precision, scale)
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							Decimal.castFrom("0", precision, scale)
					)
			);
		}

		@Override
		protected List<Decimal> getExpectedResults() {
			return Arrays.asList(
					Decimal.castFrom("-999.999", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, MinWithRetractAccumulator<Decimal>> getAggregator() {
			return new DecimalMinWithRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringMinWithRetractAggFunction.
	 */
	public static class StringMinWithRetractAggFunctionTest
			extends MinWithRetractAggFunctionTest<BinaryString> {

		@Override
		protected List<List<BinaryString>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							BinaryString.fromString("abc"),
							BinaryString.fromString("def"),
							BinaryString.fromString("ghi"),
							null,
							BinaryString.fromString("jkl"),
							null,
							BinaryString.fromString("zzz")
					),
					Arrays.asList(
							null,
							null
					),
					Arrays.asList(
							null,
							BinaryString.fromString("a")
					),
					Arrays.asList(
							BinaryString.fromString("x"),
							null,
							BinaryString.fromString("e")
					)
			);
		}

		@Override
		protected List<BinaryString> getExpectedResults() {
			return Arrays.asList(
					BinaryString.fromString("abc"),
					null,
					BinaryString.fromString("a"),
					BinaryString.fromString("e")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, MinWithRetractAccumulator<BinaryString>> getAggregator() {
			return new StringMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for TimestampMinWithRetractAggFunction.
	 */
	public static class TimestampMinWithRetractAggFunctionTest
			extends MinWithRetractAggFunctionTest<Timestamp> {

		@Override
		protected List<List<Timestamp>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							new Timestamp(0),
							new Timestamp(1000),
							new Timestamp(100),
							null,
							new Timestamp(10)
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							new Timestamp(1)
					)
			);
		}

		@Override
		protected List<Timestamp> getExpectedResults() {
			return Arrays.asList(
					new Timestamp(0),
					null,
					new Timestamp(1)
			);
		}

		@Override
		protected AggregateFunction<Timestamp, MinWithRetractAccumulator<Timestamp>> getAggregator() {
			return new TimestampMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for DateMinWithRetractAggFunction.
	 */
	public static class DateMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest<Date> {

		@Override
		protected List<List<Date>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							new Date(0),
							new Date(1000),
							new Date(100),
							null,
							new Date(10)
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							new Date(1)
					)
			);
		}

		@Override
		protected List<Date> getExpectedResults() {
			return Arrays.asList(
					new Date(0),
					null,
					new Date(1)
			);
		}

		@Override
		protected AggregateFunction<Date, MinWithRetractAccumulator<Date>> getAggregator() {
			return new DateMinWithRetractAggFunction();
		}
	}

	/**
	 * Test for TimeMinWithRetractAggFunction.
	 */
	public static class TimeMinWithRetractAggFunctionTest extends MinWithRetractAggFunctionTest<Time> {

		@Override
		protected List<List<Time>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							new Time(0),
							new Time(1000),
							new Time(100),
							null,
							new Time(10)
					),
					Arrays.asList(
							null,
							null,
							null,
							null,
							null
					),
					Arrays.asList(
							null,
							new Time(1)
					)
			);
		}

		@Override
		protected List<Time> getExpectedResults() {
			return Arrays.asList(
					new Time(0),
					null,
					new Time(1)
			);
		}

		@Override
		protected AggregateFunction<Time, MinWithRetractAccumulator<Time>> getAggregator() {
			return new TimeMinWithRetractAggFunction();
		}
	}
}
