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
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.BooleanLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.ByteLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.DecimalLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.DoubleLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.FloatLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.IntLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.LongLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.ShortLastValueAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction.StringLastValueAggFunction;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in LastValue aggregate function.
 * This class tests `accumulate` method without order argument.
 */
public abstract class LastValueAggFunctionWithoutOrderTest<T> extends AggFunctionTestBase<T, GenericRow> {

	@Override
	protected Class<?> getAccClass() {
		return GenericRow.class;
	}

	/**
	 * Test LastValueAggFunction for number type.
	 */
	public abstract static class NumberLastValueAggFunctionWithoutOrderTest<T>
			extends LastValueAggFunctionWithoutOrderTest<T> {
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

	/**
	 * Test for ByteLastValueAggFunction.
	 */
	public static class ByteLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Byte> {

		@Override
		protected Byte getValue(String v) {
			return Byte.valueOf(v);
		}

		@Override
		protected AggregateFunction<Byte, GenericRow> getAggregator() {
			return new ByteLastValueAggFunction();
		}
	}

	/**
	 * Test for ShortLastValueAggFunction.
	 */
	public static class ShortLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Short> {

		@Override
		protected Short getValue(String v) {
			return Short.valueOf(v);
		}

		@Override
		protected AggregateFunction<Short, GenericRow> getAggregator() {
			return new ShortLastValueAggFunction();
		}
	}

	/**
	 * Test for IntLastValueAggFunction.
	 */
	public static class IntLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Integer> {

		@Override
		protected Integer getValue(String v) {
			return Integer.valueOf(v);
		}

		@Override
		protected AggregateFunction<Integer, GenericRow> getAggregator() {
			return new IntLastValueAggFunction();
		}
	}

	/**
	 * Test for LongLastValueAggFunction.
	 */
	public static class LongLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Long> {

		@Override
		protected Long getValue(String v) {
			return Long.valueOf(v);
		}

		@Override
		protected AggregateFunction<Long, GenericRow> getAggregator() {
			return new LongLastValueAggFunction();
		}
	}

	/**
	 * Test for FloatLastValueAggFunction.
	 */
	public static class FloatLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Float> {

		@Override
		protected Float getValue(String v) {
			return Float.valueOf(v);
		}

		@Override
		protected AggregateFunction<Float, GenericRow> getAggregator() {
			return new FloatLastValueAggFunction();
		}
	}

	/**
	 * Test for DoubleLastValueAggFunction.
	 */
	public static class DoubleLastValueAggFunctionWithoutOrderTest
			extends NumberLastValueAggFunctionWithoutOrderTest<Double> {

		@Override
		protected Double getValue(String v) {
			return Double.valueOf(v);
		}

		@Override
		protected AggregateFunction<Double, GenericRow> getAggregator() {
			return new DoubleLastValueAggFunction();
		}
	}

	/**
	 * Test for BooleanLastValueAggFunction.
	 */
	public static class BooleanLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTest<Boolean> {

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
			return new BooleanLastValueAggFunction();
		}
	}

	/**
	 * Test for DecimalLastValueAggFunction.
	 */
	public static class DecimalLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTest<Decimal> {

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
					Decimal.castFrom("999.999", precision, scale),
					null,
					Decimal.castFrom("0", precision, scale)
			);
		}

		@Override
		protected AggregateFunction<Decimal, GenericRow> getAggregator() {
			return new DecimalLastValueAggFunction(DecimalTypeInfo.of(precision, scale));
		}
	}

	/**
	 * Test for StringLastValueAggFunction.
	 */
	public static class StringLastValueAggFunctionWithoutOrderTest extends
			LastValueAggFunctionWithoutOrderTest<BinaryString> {

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
							BinaryString.fromString("a"),
							null
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
					BinaryString.fromString("e")
			);
		}

		@Override
		protected AggregateFunction<BinaryString, GenericRow> getAggregator() {
			return new StringLastValueAggFunction();
		}
	}
}
