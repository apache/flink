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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggWsWithRetractAggFunction.ListAggWsWithRetractAccumulator;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test case for built-in concatWs with retraction aggregate function.
 */
public class ListAggWsWithRetractAggFunctionTest
	extends AggFunctionTestBase<BinaryString, ListAggWsWithRetractAccumulator> {

	@Override
	protected List<List<BinaryString>> getInputValueSets() {
		return Arrays.asList(
				Arrays.asList(
						BinaryString.fromString("a"), BinaryString.fromString("\n"),
						BinaryString.fromString("b"), BinaryString.fromString("\n"),
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("c"), BinaryString.fromString("\n"),
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("d"), BinaryString.fromString("\n"),
						BinaryString.fromString("e"), BinaryString.fromString("\n"),
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("f"), BinaryString.fromString("\n")),
				Arrays.asList(null, null, null, null, null, null),
				Arrays.asList(
						null, BinaryString.fromString("\n"),
						null, BinaryString.fromString("\n"), null,
						BinaryString.fromString("\n")),
				Arrays.asList(
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("a"), BinaryString.fromString("\n"),
						BinaryString.fromString("b"), BinaryString.fromString("\n")),
				Arrays.asList(
						BinaryString.fromString("a"), BinaryString.fromString(","),
						BinaryString.fromString("b"), BinaryString.fromString(","),
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("c"), BinaryString.fromString(",")),
				Arrays.asList(
						BinaryString.fromString("a"), BinaryString.fromString(","),
						BinaryString.fromString("b"), BinaryString.fromString(","),
						null, BinaryString.fromString("\n"),
						BinaryString.fromString("c"), BinaryString.fromString("\n"))
		);
	}

	@Override
	protected List<BinaryString> getExpectedResults() {
		return Arrays.asList(
				BinaryString.fromString("a\nb\nc\nd\ne\nf"),
				null,
				null,
				BinaryString.fromString("a\nb"),
				BinaryString.fromString("a,b,c"),
				BinaryString.fromString("a\nb\nc"));
	}

	@Override
	protected AggregateFunction<BinaryString, ListAggWsWithRetractAccumulator> getAggregator() {
		return new ListAggWsWithRetractAggFunction();
	}

	@Override
	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod(
				"accumulate", getAccClass(), BinaryString.class, BinaryString.class);
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), BinaryString.class, BinaryString.class);
	}

	@Override
	protected Class<?> getAccClass() {
		return ListAggWsWithRetractAccumulator.class;
	}

	@Override
	protected <E> void validateResult(E expected, E result) {
		if (expected instanceof ListAggWsWithRetractAccumulator && result instanceof ListAggWsWithRetractAccumulator) {
			ListAggWsWithRetractAccumulator e = (ListAggWsWithRetractAccumulator) expected;
			ListAggWsWithRetractAccumulator r = (ListAggWsWithRetractAccumulator) result;
			assertEquals(e.list, r.list);
			assertEquals(e.list, r.list);
		} else {
			super.validateResult(expected, result);
		}
	}

	@Override
	protected ListAggWsWithRetractAccumulator accumulateValues(List<BinaryString> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<BinaryString, ListAggWsWithRetractAccumulator> aggregator = getAggregator();
		ListAggWsWithRetractAccumulator accumulator = getAggregator().createAccumulator();
		Method accumulateFunc = getAccumulateFunc();
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		for (int i = 0; i < values.size(); i += 2) {
			BinaryString value = values.get(i + 1);
			BinaryString delimiter = values.get(i);
			accumulateFunc.invoke(aggregator, accumulator, delimiter, value);
		}
		return accumulator;
	}

	@Override
	protected void retractValues(ListAggWsWithRetractAccumulator accumulator, List<BinaryString> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<BinaryString, ListAggWsWithRetractAccumulator> aggregator = getAggregator();
		Method retractFunc = getRetractFunc();
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		for (int i = 0; i < values.size(); i += 2) {
			BinaryString value = values.get(i + 1);
			BinaryString delimiter = values.get(i);
			retractFunc.invoke(aggregator, accumulator, delimiter, value);
		}
	}

	@Override
	protected Tuple2<List<BinaryString>, List<BinaryString>> splitValues(List<BinaryString> values) {
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		int index = values.size() / 2;
		if (index % 2 != 0) {
			index -= 1;
		}
		return super.splitValues(values, index);
	}
}
