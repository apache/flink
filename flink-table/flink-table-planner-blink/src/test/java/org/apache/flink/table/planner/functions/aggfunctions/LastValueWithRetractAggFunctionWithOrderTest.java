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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in LastValue with retract aggregate function.
 * This class tests `accumulate` method with order argument.
 */
public abstract class LastValueWithRetractAggFunctionWithOrderTest<T>
		extends FirstLastValueAggFunctionWithOrderTestBase<T> {

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class, Long.class);
	}

	/**
	 * Test LastValueWithRetractAggFunction for number type.
	 */
	public abstract static class NumberLastValueWithRetractAggFunctionWithOrderTest<T>
			extends LastValueWithRetractAggFunctionWithOrderTest<T> {
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
							13L,
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
					getValue("10")
			);
		}
	}

	/**
	 * Test for ByteLastValueWithRetractAggFunction.
	 */
	public static class ByteLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for ShortLastValueWithRetractAggFunction.
	 */
	public static class ShortLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for IntLastValueWithRetractAggFunction.
	 */
	public static class IntLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for LongLastValueWithRetractAggFunction.
	 */
	public static class LongLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for FloatLastValueWithRetractAggFunction.
	 */
	public static class FloatLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for DoubleLastValueWithRetractAggFunction.
	 */
	public static class DoubleLastValueWithRetractAggFunctionWithOrderTest
			extends NumberLastValueWithRetractAggFunctionWithOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for BooleanLastValueWithRetractAggFunction.
	 */
	public static class BooleanLastValueWithRetractAggFunctionWithOrderTest
			extends LastValueWithRetractAggFunctionWithOrderTest<Boolean> {

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
					true,
					null,
					true
			);
		}

		@Override
		protected AggregateFunction<Boolean, GenericRow> getAggregator() {
			return new BooleanLastValueWithRetractAggFunction();
		}
	}

	/**
	 * Test for DecimalLastValueWithRetractAggFunction.
	 */
	public static class DecimalLastValueWithRetractAggFunctionWithOrderTest
			extends LastValueWithRetractAggFunctionWithOrderTest<Decimal> {

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
					Decimal.castFrom("1", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, GenericRow> getAggregator() {
			return new DecimalLastValueWithRetractAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringLastValueWithRetractAggFunction.
	 */
	public static class StringLastValueWithRetractAggFunctionWithOrderTest
			extends LastValueWithRetractAggFunctionWithOrderTest<BinaryString> {

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
							BinaryString.fromString("zzz"),
							BinaryString.fromString("abc"),
							BinaryString.fromString("def"),
							BinaryString.fromString("abc")
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
							5L,
							10L,
							15L,
							11L
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
					BinaryString.fromString("x")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringLastValueWithRetractAggFunction();
		}
	}
}
