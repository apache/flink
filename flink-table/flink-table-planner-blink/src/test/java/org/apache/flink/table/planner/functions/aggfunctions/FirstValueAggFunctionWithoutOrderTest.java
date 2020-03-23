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
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.BooleanFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.ByteFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.DecimalFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.DoubleFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.FloatFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.IntFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.LongFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.ShortFirstValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueAggFunction.StringFirstValueAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in FirstValue aggregate function.
 * This class tests `accumulate` method without order argument.
 */
@RunWith(Enclosed.class)
public final class FirstValueAggFunctionWithoutOrderTest {

	// --------------------------------------------------------------------------------------------
	// Test sets for a particular type being aggregated
	//
	// Actual tests are implemented in:
	//  - AggFunctionTestBase
	// --------------------------------------------------------------------------------------------

	/**
	 * Test for ByteFirstValueAggFunction.
	 */
	public static final class ByteFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteFirstValueAggFunction();
		}
	}

	/**
	 * Test for ShortFirstValueAggFunction.
	 */
	public static final class ShortFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortFirstValueAggFunction();
		}
	}

	/**
	 * Test for IntFirstValueAggFunction.
	 */
	public static final class IntFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntFirstValueAggFunction();
		}
	}

	/**
	 * Test for LongFirstValueAggFunction.
	 */
	public static final class LongFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongFirstValueAggFunction();
		}
	}

	/**
	 * Test for FloatFirstValueAggFunction.
	 */
	public static final class FloatFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatFirstValueAggFunction();
		}
	}

	/**
	 * Test for DoubleFirstValueAggFunction.
	 */
	public static final class DoubleFirstValueAggFunctionWithoutOrderTest
			extends NumberFirstValueAggFunctionWithoutOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleFirstValueAggFunction();
		}
	}

	/**
	 * Test for BooleanFirstValueAggFunction.
	 */
	public static final class BooleanFirstValueAggFunctionWithoutOrderTest extends
			FirstValueAggFunctionWithoutOrderTestBase<Boolean> {

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
		protected AggregateFunction<Boolean, GenericRow> getAggregator() {
			return new BooleanFirstValueAggFunction();
		}
	}

	/**
	 * Test for DecimalFirstValueAggFunction.
	 */
	public static final class DecimalFirstValueAggFunctionWithoutOrderTest extends
			FirstValueAggFunctionWithoutOrderTestBase<Decimal> {

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
					Decimal.castFrom("1", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, GenericRow> getAggregator() {
			return new DecimalFirstValueAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringFirstValueAggFunction.
	 */
	public static final class StringFirstValueAggFunctionWithoutOrderTest extends
			FirstValueAggFunctionWithoutOrderTestBase<BinaryString> {

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
					BinaryString.fromString("x")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringFirstValueAggFunction();
		}
	}

	// --------------------------------------------------------------------------------------------
	// This section contain base classes that provide:
	//  - common inputs
	//  - accumulator class
	// for tests declared above.
	// --------------------------------------------------------------------------------------------

	/**
	 * The base test class for FirstValueAggFunction without order.
	 */
	public abstract static class FirstValueAggFunctionWithoutOrderTestBase<T>
		extends AggFunctionTestBase<T, GenericRow> {
		@Override
		protected Class<?> getAccClass() {
			return GenericRow.class;
		}
	}

	/**
	 * Test FirstValueAggFunction for number type.
	 */
	public abstract static class NumberFirstValueAggFunctionWithoutOrderTest<T>
		extends FirstValueAggFunctionWithoutOrderTestBase<T> {
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
				getValue("1"),
				null,
				getValue("10")
			);
		}
	}
}
