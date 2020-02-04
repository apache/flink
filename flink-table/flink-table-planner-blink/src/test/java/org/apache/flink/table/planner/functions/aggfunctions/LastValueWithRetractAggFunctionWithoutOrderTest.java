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
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.BooleanLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.ByteLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.DecimalLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.DoubleLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.FloatLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.IntLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.LongLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.ShortLastValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueWithRetractAggFunction.StringLastValueWithRetractAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Test case for built-in LastValue with retract aggregate function.
 * This class tests `accumulate` method without order argument.
 */
@RunWith(Parameterized.class)
public class LastValueWithRetractAggFunctionWithoutOrderTest<T> extends AggFunctionTestBase<T, GenericRow> {

	@Parameterized.Parameter
	public AggFunctionTestSpec<T, GenericRow> aggFunctionTestSpec;

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
	protected AggregateFunction<T, GenericRow> getAggregator() {
		return aggFunctionTestSpec.aggregator;
	}

	@Override
	protected Class<?> getAccClass() {
		return GenericRow.class;
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
	}

	@Parameterized.Parameters(name = "{index}: {0}")
	public static List<AggFunctionTestSpec> testData() {
		return Arrays.asList(
				/**
				 * Test for ByteLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new ByteLastValueWithRetractAggFunction(),
						numberInputValueSets(Byte::valueOf),
						numberExpectedResults(Byte::valueOf)
				),
				/**
				 * Test for ShortLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new ShortLastValueWithRetractAggFunction(),
						numberInputValueSets(Short::valueOf),
						numberExpectedResults(Short::valueOf)
				),
				/**
				 * Test for IntLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new IntLastValueWithRetractAggFunction(),
						numberInputValueSets(Integer::valueOf),
						numberExpectedResults(Integer::valueOf)
				),
				/**
				 * Test for LongLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new LongLastValueWithRetractAggFunction(),
						numberInputValueSets(Long::valueOf),
						numberExpectedResults(Long::valueOf)
				),
				/**
				 * Test for FloatLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new FloatLastValueWithRetractAggFunction(),
						numberInputValueSets(Float::valueOf),
						numberExpectedResults(Float::valueOf)
				),
				/**
				 * Test for DoubleLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new DoubleLastValueWithRetractAggFunction(),
						numberInputValueSets(Double::valueOf),
						numberExpectedResults(Double::valueOf)
				),
				/**
				 * Test for BooleanLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new BooleanLastValueWithRetractAggFunction(),
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
								true,
								null,
								true
						)
				),
				/**
				 * Test for DecimalLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new DecimalLastValueWithRetractAggFunction(
								DecimalTypeInfo.of(DECIMAL_PRECISION, DECIMAL_SCALE)),
						Arrays.asList(
								Arrays.asList(
										Decimal.castFrom("1", DECIMAL_PRECISION, DECIMAL_SCALE),
										Decimal.castFrom("1000.000001", DECIMAL_PRECISION, DECIMAL_SCALE),
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
								Decimal.castFrom("999.999", DECIMAL_PRECISION, DECIMAL_SCALE),
								null,
								Decimal.castFrom("0", DECIMAL_PRECISION, DECIMAL_SCALE)
						)
				),
				/**
				 * Test for StringLastValueWithRetractAggFunction.
				 */
				new AggFunctionTestSpec<>(
						new StringLastValueWithRetractAggFunction(),
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
								BinaryString.fromString("zzz"),
								null,
								BinaryString.fromString("a"),
								BinaryString.fromString("e")
						)
				)

		);
	}

	private static <N> List<List<N>> numberInputValueSets(Function<String, N> strToValueFun) {
		return Arrays.asList(
				Arrays.asList(
						strToValueFun.apply("1"),
						null,
						strToValueFun.apply("-99"),
						strToValueFun.apply("3"),
						null
				),
				Arrays.asList(
						null,
						null,
						null,
						null
				),
				Arrays.asList(
						null,
						strToValueFun.apply("10"),
						null,
						strToValueFun.apply("3")
				)
		);
	}

	private static <N> List<N> numberExpectedResults(Function<String, N> strToValueFun) {
		return Arrays.asList(
				strToValueFun.apply("3"),
				null,
				strToValueFun.apply("3")
		);
	}
}
