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
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.BooleanFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.ByteFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.DecimalFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.DoubleFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.FloatFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.IntFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.LongFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.ShortFirstValueWithRetractAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.FirstValueWithRetractAggFunction.StringFirstValueWithRetractAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in FirstValue with retract aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class FirstValueWithRetractAggFunctionWithOrderTest<T>
		extends FirstLastValueAggFunctionWithOrderTestBase<T> {

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class, Long.class);
	}

	/**
	 * Test FirstValueWithRetractAggFunction for number type.
	 */
	public abstract static class NumberFirstValueWithRetractAggFunctionWithOrderTest<T>
			extends FirstValueWithRetractAggFunctionWithOrderTest<T> {
		protected abstract T getValue(String v);

		@Override
		protected List<List<T>> getInputValueSets() {
			return Arrays.asList(
					Arrays.asList(
							getValue("1"),
							null,
							getValue("-99"),
							getValue("3"),
							null,
							getValue("3"),
							getValue("2"),
							getValue("-99")
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
							getValue("5")
					)
			);
		}

		@Override
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							5L,
							6L,
							11L,
							3L,
							7L,
							5L
					),
					Arrays.asList(
							8L,
							6L,
							9L,
							5L
					),
					Arrays.asList(
							null,
							6L,
							4L,
							3L
					)
			);
		}

		@Override
		protected List<T> getExpectedResults() {
			return Arrays.asList(
					getValue("3"),
					null,
					getValue("5")
			);
		}
	}

	/**
	 * Test for ByteFirstValueWithRetractAggFunction.
	 */
	public static class ByteFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for ShortFirstValueWithRetractAggFunction.
	 */
	public static class ShortFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for IntFirstValueWithRetractAggFunction.
	 */
	public static class IntFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for LongFirstValueWithRetractAggFunction.
	 */
	public static class LongFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for FloatFirstValueWithRetractAggFunction.
	 */
	public static class FloatFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleFirstValueWithRetractAggFunction.
	 */
	public static class DoubleFirstValueWithRetractAggFunctionWithOrderTest
			extends NumberFirstValueWithRetractAggFunctionWithOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanFirstValueWithRetractAggFunction.
	 */
	public static class BooleanFirstValueWithRetractAggFunctionWithOrderTest
			extends FirstValueWithRetractAggFunctionWithOrderTest<Boolean> {

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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							6L,
							2L,
							3L
					),
					Arrays.asList(
							1L,
							2L,
							3L
					),
					Arrays.asList(
							10L,
							2L,
							5L,
							11L,
							3L,
							7L,
							5L
					),
					Arrays.asList(
							6L,
							9L,
							5L
					),
					Arrays.asList(
							4L,
							3L
					)
			);
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
		protected AggregateFunction<Boolean, GenericRow> getAggregator() {
			return new BooleanFirstValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalFirstValueWithRetractAggFunction.
	 */
	public static class DecimalFirstValueWithRetractAggFunctionWithOrderTest
			extends FirstValueWithRetractAggFunctionWithOrderTest<Decimal> {

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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							1L,
							5L,
							null,
							3L,
							1L,
							5L,
							2L
					),
					Arrays.asList(
							6L,
							5L,
							null,
							8L,
							null
					),
					Arrays.asList(
							8L,
							6L
					)
			);
		}

		@Override
		protected List<Decimal> getExpectedResults() {
			return Arrays.asList(
					Decimal.castFrom("-1", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, GenericRow> getAggregator() {
			return new DecimalFirstValueWithRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringFirstValueWithRetractAggFunction.
	 */
	public static class StringFirstValueWithRetractAggFunctionWithOrderTest
			extends FirstValueWithRetractAggFunctionWithOrderTest<BinaryString> {

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
		protected List<List<Long>> getInputOrderSets() {
			return Arrays.asList(
					Arrays.asList(
							10L,
							2L,
							5L,
							null,
							3L,
							1L,
							5L
					),
					Arrays.asList(
							6L,
							5L
					),
					Arrays.asList(
							8L,
							6L
					),
					Arrays.asList(
							6L,
							4L,
							3L
					)
			);
		}

		@Override
		protected List<BinaryString> getExpectedResults() {
			return Arrays.asList(
					BinaryString.fromString("def"),
					null,
					BinaryString.fromString("a"),
					BinaryString.fromString("e")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringFirstValueWithRetractAggFunction();
		}
	}
}
