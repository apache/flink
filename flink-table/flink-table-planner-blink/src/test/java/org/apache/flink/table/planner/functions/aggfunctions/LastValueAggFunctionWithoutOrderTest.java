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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.BooleanLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.ByteLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.DecimalLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.DoubleLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.FloatLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.IntLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.LongLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.ShortLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.StringLastValueAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in LastValue aggregate function.
 * This class tests `accumulate` method without order argument.
 */
@RunWith(Enclosed.class)
public final class LastValueAggFunctionWithoutOrderTest {

	// --------------------------------------------------------------------------------------------
	// Test sets for a particular type being aggregated
	//
	// Actual tests are implemented in:
	//  - AggFunctionTestBase
	// --------------------------------------------------------------------------------------------

	/**
	 * Test for ByteLastValueAggFunction.
	 */
	public static final class ByteLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRowData> getAggregator() {
			return new ByteLastValueAggFunction();
		}
	}

	/**
	 * Test for ShortLastValueAggFunction.
	 */
	public static final class ShortLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRowData> getAggregator() {
			return new ShortLastValueAggFunction();
		}
	}

	/**
	 * Test for IntLastValueAggFunction.
	 */
	public static final class IntLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRowData> getAggregator() {
			return new IntLastValueAggFunction();
		}
	}

	/**
	 * Test for LongLastValueAggFunction.
	 */
	public static final class LongLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRowData> getAggregator() {
			return new LongLastValueAggFunction();
		}
	}

	/**
	 * Test for FloatLastValueAggFunction.
	 */
	public static final class FloatLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRowData> getAggregator() {
			return new FloatLastValueAggFunction();
		}
	}

	/**
	 * Test for DoubleLastValueAggFunction.
	 */
	public static final class DoubleLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTestBase<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRowData> getAggregator() {
			return new DoubleLastValueAggFunction();
		}
	}

	/**
	 * Test for BooleanLastValueAggFunction.
	 */
	public static final class BooleanLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTestBase<Boolean> {

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
		protected AggregateFunction<Boolean, GenericRowData> getAggregator() {
			return new BooleanLastValueAggFunction();
		}
	}

	/**
	 * Test for DecimalLastValueAggFunction.
	 */
	public static final class DecimalLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTestBase<DecimalData> {

		private int precision = 20;
		private int scale = 6;

		@Override
		protected List<List<DecimalData>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							DecimalDataUtils.castFrom("1", precision, scale),
							DecimalDataUtils.castFrom("1000.000001", precision, scale),
							DecimalDataUtils.castFrom("-1", precision, scale),
							DecimalDataUtils.castFrom("-999.998999", precision, scale),
							null,
							DecimalDataUtils.castFrom("0", precision, scale),
							DecimalDataUtils.castFrom("-999.999", precision, scale),
							null,
							DecimalDataUtils.castFrom("999.999", precision, scale)
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
							DecimalDataUtils.castFrom("0", precision, scale)
					)
			);
		}

		@Override
		protected List<DecimalData> getExpectedResults() {
			return Arrays.asList(
					DecimalDataUtils.castFrom("999.999", precision, scale),
					null,
					DecimalDataUtils.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<DecimalData, GenericRowData> getAggregator() {
			return new DecimalLastValueAggFunction(DecimalDataTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringLastValueAggFunction.
	 */
	public static final class StringLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTestBase<StringData> {

		@Override
		protected List<List<StringData>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							StringData.fromString("abc"),
							StringData.fromString("def"),
							StringData.fromString("ghi"),
							null,
							StringData.fromString("jkl"),
							null,
							StringData.fromString("zzz")
					),
					Arrays.asList(
							null,
							null
					),
					Arrays.asList(
							null,
							StringData.fromString("a"),
							null
					),
					Arrays.asList(
							StringData.fromString("x"),
							null,
							StringData.fromString("e")
					)
			);
		}

		@Override
		protected List<StringData> getExpectedResults() {
			return Arrays.asList(
					StringData.fromString("zzz"),
					null,
					StringData.fromString("a"),
					StringData.fromString("e")
			);
		}

		@Override
		protected AggregateFunction<StringData, GenericRowData> getAggregator() {
			return new StringLastValueAggFunction();
		}
	}

	// --------------------------------------------------------------------------------------------
	// This section contain base classes that provide common inputs and declare the accumulator
	// class type for tests declared above.
	// --------------------------------------------------------------------------------------------

	/**
	 * The base test class for LastValueAggFunction without order.
	 */
	public abstract static class LastValueAggFunctionWithoutOrderTestBase<T>
		extends AggFunctionTestBase<T, GenericRowData> {

		@Override
		protected Class<?> getAccClass() {
			return GenericRowData.class;
		}
	}

	/**
	 * Test LastValueAggFunction for number type.
	 */
	public abstract static class NumberLastValueAggFunctionWithoutOrderTestBase<T>
		extends LastValueAggFunctionWithoutOrderTestBase<T> {
		protected abstract T getValue(String v);

		@Override
		protected List<List<T>> getInputValueSets() {
			return Arrays.asList(
				Arrays.asList(
					getValue("1"),
					null,
					getValue("-99"),
					getValue("3"),
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
					getValue("10"),
					null,
					getValue("3")
				)
			);
		}

		@Override
		protected List<T> getExpectedResults() {
			return Arrays.asList(
				getValue("3"),
				null,
				getValue("3")
			);
		}
	}
}
