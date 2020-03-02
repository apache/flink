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
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.BooleanMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.ByteMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.DateMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.DecimalMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.DoubleMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.FloatMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.IntMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.LongMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.MaxWithRetractAccumulator;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.ShortMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.StringMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.TimeMaxWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxWithRetractAggFunction.TimestampMaxWithRetractAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Time;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in Max with retraction aggregate function.
 */
@RunWith(Enclosed.class)
public final class MaxWithRetractAggFunctionTest {

	// --------------------------------------------------------------------------------------------
	// Test sets for a particular type being aggregated
	//
	// Actual tests are implemented in:
	//  - AggFunctionTestBase
	// --------------------------------------------------------------------------------------------

	/**
	 * Test for ByteMaxWithRetractAggFunction.
	 */
	public static final class ByteMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Byte> {

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
		protected AggregateFunction<Byte, MaxWithRetractAccumulator<Byte>> getAggregator() {
			return new ByteMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for ShortMaxWithRetractAggFunction.
	 */
	public static final class ShortMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Short> {

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
		protected AggregateFunction<Short, MaxWithRetractAccumulator<Short>> getAggregator() {
			return new ShortMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for IntMaxWithRetractAggFunction.
	 */
	public static final class IntMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Integer> {

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
		protected AggregateFunction<Integer, MaxWithRetractAccumulator<Integer>> getAggregator() {
			return new IntMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for LongMaxWithRetractAggFunction.
	 */
	public static final class LongMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Long> {

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
		protected AggregateFunction<Long, MaxWithRetractAccumulator<Long>> getAggregator() {
			return new LongMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for FloatMaxWithRetractAggFunction.
	 */
	public static final class FloatMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Float> {

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
		protected AggregateFunction<Float, MaxWithRetractAccumulator<Float>> getAggregator() {
			return new FloatMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleMaxWithRetractAggFunction.
	 */
	public static final class DoubleMaxWithRetractAggFunctionTest extends NumberMaxWithRetractAggFunctionTest<Double> {

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
		protected AggregateFunction<Double, MaxWithRetractAccumulator<Double>> getAggregator() {
			return new DoubleMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanMaxWithRetractAggFunction.
	 */
	public static final class BooleanMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<Boolean> {

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
					true,
					null,
					true
			);
		}

		@Override
		protected AggregateFunction<Boolean, MaxWithRetractAccumulator<Boolean>> getAggregator() {
			return new BooleanMaxWithRetractAggFunction();
		}

		@Override
		protected Class<?> getAccClass() {
			return MaxWithRetractAccumulator.class;
		}

		@Override
		protected Method getRetractFunc() throws NoSuchMethodException {
			return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
		}
	}

	/**
	 * Test for DecimalMaxWithRetractAggFunction.
	 */
	public static final class DecimalMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<Decimal> {

		private int precision = 20;
		private int scale = 6;

		@Override
		protected List<List<Decimal>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							Decimal.castFrom("1", precision, scale),
							Decimal.castFrom("1000.000001", precision, scale),
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
					Decimal.castFrom("1000.000001", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, MaxWithRetractAccumulator<Decimal>> getAggregator() {
			return new DecimalMaxWithRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringMaxWithRetractAggFunction.
	 */
	public static final class StringMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<BinaryString> {

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
					BinaryString.fromString("zzz"),
					null,
					BinaryString.fromString("a"),
					BinaryString.fromString("x")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, MaxWithRetractAccumulator<BinaryString>> getAggregator() {
			return new StringMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for TimestampMaxWithRetractAggFunction.
	 */
	public static final class TimestampMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<SqlTimestamp> {

		@Override
		protected List<List<SqlTimestamp>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							SqlTimestamp.fromEpochMillis(0),
							SqlTimestamp.fromEpochMillis(1000),
							SqlTimestamp.fromEpochMillis(100),
							null,
							SqlTimestamp.fromEpochMillis(10)
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
							SqlTimestamp.fromEpochMillis(1)
					)
			);
		}

		@Override
		protected List<SqlTimestamp> getExpectedResults() {
			return Arrays.asList(
					SqlTimestamp.fromEpochMillis(1000),
					null,
					SqlTimestamp.fromEpochMillis(1)
			);
		}

		@Override
		protected AggregateFunction<SqlTimestamp, MaxWithRetractAccumulator<SqlTimestamp>> getAggregator() {
			return new TimestampMaxWithRetractAggFunction(3);
		}
	}

	/**
	 * Test for TimestampMaxWithRetractAggFunction, precision is 9.
	 */
	public static final class Timestamp9MaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<SqlTimestamp> {

		@Override
		protected List<List<SqlTimestamp>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							SqlTimestamp.fromEpochMillis(0, 0),
							SqlTimestamp.fromEpochMillis(1000, 0),
							SqlTimestamp.fromEpochMillis(1000, 1),
							SqlTimestamp.fromEpochMillis(100, 0),
							null,
							SqlTimestamp.fromEpochMillis(10, 0)
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
							SqlTimestamp.fromEpochMillis(1, 0),
							SqlTimestamp.fromEpochMillis(1, 1)
					)
			);
		}

		@Override
		protected List<SqlTimestamp> getExpectedResults() {
			return Arrays.asList(
					SqlTimestamp.fromEpochMillis(1000, 1),
					null,
					SqlTimestamp.fromEpochMillis(1, 1)
			);
		}

		@Override
		protected AggregateFunction<SqlTimestamp, MaxWithRetractAccumulator<SqlTimestamp>> getAggregator() {
			return new TimestampMaxWithRetractAggFunction(9);
		}
	}

	/**
	 * Test for DateMaxWithRetractAggFunction.
	 */
	public static final class DateMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<Date> {

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
					new Date(1000),
					null,
					new Date(1)
			);
		}

		@Override
		protected AggregateFunction<Date, MaxWithRetractAccumulator<Date>> getAggregator() {
			return new DateMaxWithRetractAggFunction();
		}
	}

	/**
	 * Test for TimeMaxWithRetractAggFunction.
	 */
	public static final class TimeMaxWithRetractAggFunctionTest extends MaxWithRetractAggFunctionTestBase<Time> {

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
					new Time(1000),
					null,
					new Time(1)
			);
		}

		@Override
		protected AggregateFunction<Time, MaxWithRetractAccumulator<Time>> getAggregator() {
			return new TimeMaxWithRetractAggFunction();
		}
	}

	// --------------------------------------------------------------------------------------------
	// This section contain base classes that provide:
	//  - common inputs
	//  - declare the accumulator class
	//  - accessor for retract function
	//  for tests declared above.
	// --------------------------------------------------------------------------------------------

	/**
	 * The base test class for MaxWithRetractAggFunction.
	 */
	public abstract static class MaxWithRetractAggFunctionTestBase<T>
		extends AggFunctionTestBase<T, MaxWithRetractAccumulator<T>> {

		@Override
		protected Class<?> getAccClass() {
			return MaxWithRetractAccumulator.class;
		}

		@Override
		protected Method getRetractFunc() throws NoSuchMethodException {
			return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
		}
	}

	/**
	 * Test MaxWithRetractAggFunction for number type.
	 */
	public abstract static class NumberMaxWithRetractAggFunctionTest<T> extends MaxWithRetractAggFunctionTestBase<T> {
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
				getMaxValue(),
				null,
				getValue("10")
			);
		}
	}
}
