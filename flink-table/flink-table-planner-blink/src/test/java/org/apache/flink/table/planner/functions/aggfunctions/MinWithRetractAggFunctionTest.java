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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Time;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Test case for built-in Min with retraction aggregate function.
 */
@RunWith(Parameterized.class)
public class MinWithRetractAggFunctionTest<T> extends AggFunctionTestBase<T, MinWithRetractAccumulator<T>> {

	@Parameterized.Parameter
	public AggFunctionTestSpec<T, MinWithRetractAccumulator<T>> aggFunctionTestSpec;

	private static final int DECIMAL_PRECISION = 20;
	private static final int DECIMAL_SCALE = 6;

	@Override
	protected List<List<T>> getInputValueSets() {
		return aggFunctionTestSpec.inputValueSets;
	}

	@Override
	protected List<T> getExpectedResults() {
		return aggFunctionTestSpec.expectedResults;
	}

	@Override
	protected AggregateFunction<T, MinWithRetractAccumulator<T>> getAggregator() {
		return aggFunctionTestSpec.aggregator;
	}

	@Override
	protected Class<?> getAccClass() {
		return MinWithRetractAccumulator.class;
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
	}

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<AggFunctionTestSpec> testData() {
		return Arrays.asList(
				/**
				 * Test for ByteMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new ByteMinWithRetractAggFunction(),
						numberInputValueSets((byte) (Byte.MIN_VALUE + 1), (byte) (Byte.MAX_VALUE - 1), Byte::valueOf),
						numberExpectedResults((byte) (Byte.MIN_VALUE + 1), Byte::valueOf)
				),
				/**
				 * Test for ShortMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new ShortMinWithRetractAggFunction(),
						numberInputValueSets(
								(short) (Short.MIN_VALUE + 1), (short) (Short.MAX_VALUE - 1), Short::valueOf),
						numberExpectedResults((short) (Short.MIN_VALUE + 1), Short::valueOf)
				),
				/**
				 * Test for IntMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new IntMinWithRetractAggFunction(),
						numberInputValueSets(Integer.MIN_VALUE + 1, Integer.MAX_VALUE - 1, Integer::valueOf),
						numberExpectedResults(Integer.MIN_VALUE + 1, Integer::valueOf)
				),
				/**
				 * Test for LongMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new LongMinWithRetractAggFunction(),
						numberInputValueSets(Long.MIN_VALUE + 1L, Long.MAX_VALUE - 1L, Long::valueOf),
						numberExpectedResults(Long.MIN_VALUE + 1L, Long::valueOf)
				),
				/**
				 * Test for FloatMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new FloatMinWithRetractAggFunction(),
						numberInputValueSets((-Float.MAX_VALUE / 2), (Float.MAX_VALUE / 2), Float::valueOf),
						numberExpectedResults((-Float.MAX_VALUE / 2), Float::valueOf)
				),
				/**
				 * Test for DoubleMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new DoubleMinWithRetractAggFunction(),
						numberInputValueSets((-Double.MAX_VALUE / 2), (Double.MAX_VALUE / 2), Double::valueOf),
						numberExpectedResults((-Double.MAX_VALUE / 2), Double::valueOf)
				),
				/**
				 * Test for BooleanMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new BooleanMinWithRetractAggFunction(),
						Arrays.asList(
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
								)
						),
						Arrays.asList(
								false,
								true,
								false,
								null,
								true
						)
				),
				/**
				 * Test for DecimalMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new DecimalMinWithRetractAggFunction(DecimalTypeInfo.of(DECIMAL_PRECISION, DECIMAL_SCALE)),
						Arrays.asList(
								Arrays.asList(
										Decimal.castFrom("1", DECIMAL_PRECISION, DECIMAL_SCALE),
										Decimal.castFrom("1000", DECIMAL_PRECISION, DECIMAL_SCALE),
										Decimal.castFrom("-1", DECIMAL_PRECISION, DECIMAL_SCALE),
										Decimal.castFrom("-999.998999", DECIMAL_PRECISION, DECIMAL_SCALE),
										null,
										Decimal.castFrom("0", DECIMAL_PRECISION, DECIMAL_SCALE),
										Decimal.castFrom("-999.999", DECIMAL_PRECISION, DECIMAL_SCALE),
										null,
										Decimal.castFrom("999.999", DECIMAL_PRECISION, DECIMAL_SCALE)
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
										Decimal.castFrom("0", DECIMAL_PRECISION, DECIMAL_SCALE)
								)
						),
						Arrays.asList(
								Decimal.castFrom("-999.999", DECIMAL_PRECISION, DECIMAL_SCALE),
								null,
								Decimal.castFrom("0", DECIMAL_PRECISION, DECIMAL_SCALE)
						)
				),
				/**
				 * Test for StringMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new StringMinWithRetractAggFunction(),
						Arrays.asList(
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
						),
						Arrays.asList(
								BinaryString.fromString("abc"),
								null,
								BinaryString.fromString("a"),
								BinaryString.fromString("e")
						)
				),
				/**
				 * Test for TimestampMinWithRetractAggFunction with millisecond's precision.
				 */
				new AggFunctionTestSpec<>(
						new TimestampMinWithRetractAggFunction(3),
						Arrays.asList(
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
						),
						Arrays.asList(
								SqlTimestamp.fromEpochMillis(0),
								null,
								SqlTimestamp.fromEpochMillis(1)
						)
				),
				/**
				 * Test for TimestampMinWithRetractAggFunction with nanosecond's precision.
				 */
				new AggFunctionTestSpec<>(
						new TimestampMinWithRetractAggFunction(9),
						Arrays.asList(
								Arrays.asList(
										SqlTimestamp.fromEpochMillis(0, 1),
										SqlTimestamp.fromEpochMillis(0, 2),
										SqlTimestamp.fromEpochMillis(1000, 0),
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
										SqlTimestamp.fromEpochMillis(1, 1),
										SqlTimestamp.fromEpochMillis(1, 2)
								)
						),
						Arrays.asList(
								SqlTimestamp.fromEpochMillis(0, 1),
								null,
								SqlTimestamp.fromEpochMillis(1, 1)
						)
				),

				/**
				 * Test for DateMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new DateMinWithRetractAggFunction(),
						Arrays.asList(
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
						),
						Arrays.asList(
								new Date(0),
								null,
								new Date(1)
						)
				),
				/**
				 * Test for TimeMinWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new TimeMinWithRetractAggFunction(),
						Arrays.asList(
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
						),
						Arrays.asList(
								new Time(0),
								null,
								new Time(1)
						)
				)
		);
	}

	private static <N> List<List<N>> numberInputValueSets(N minValue, N maxValue, Function<String, N> strToValueFun) {
		return Arrays.asList(
				Arrays.asList(
						strToValueFun.apply("1"),
						null,
						maxValue,
						strToValueFun.apply("-99"),
						strToValueFun.apply("3"),
						strToValueFun.apply("56"),
						strToValueFun.apply("0"),
						minValue,
						strToValueFun.apply("-20"),
						strToValueFun.apply("17"),
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
						strToValueFun.apply("10")
				)
		);
	}

	private static <N> List<N> numberExpectedResults(N minValue, Function<String, N> strToValueFun) {
		return Arrays.asList(
				minValue,
				null,
				strToValueFun.apply("10")
		);
	}
}
