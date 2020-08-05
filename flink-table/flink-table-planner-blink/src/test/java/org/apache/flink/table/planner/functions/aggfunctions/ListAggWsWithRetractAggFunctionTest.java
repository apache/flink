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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggWsWithRetractAggFunction.ListAggWsWithRetractAccumulator;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in ListAggWs with retraction aggregate function.
 */
public final class ListAggWsWithRetractAggFunctionTest
	extends AggFunctionTestBase<StringData, ListAggWsWithRetractAccumulator> {

	@Override
	protected List<List<StringData>> getInputValueSets() {
		return Arrays.asList(
				Arrays.asList(
						StringData.fromString("a"), StringData.fromString("\n"),
						StringData.fromString("b"), StringData.fromString("\n"),
						null, StringData.fromString("\n"),
						StringData.fromString("c"), StringData.fromString("\n"),
						null, StringData.fromString("\n"),
						StringData.fromString("d"), StringData.fromString("\n"),
						StringData.fromString("e"), StringData.fromString("\n"),
						null, StringData.fromString("\n"),
						StringData.fromString("f"), StringData.fromString("\n")),
				Arrays.asList(null, null, null, null, null, null),
				Arrays.asList(
						null, StringData.fromString("\n"),
						null, StringData.fromString("\n"), null,
						StringData.fromString("\n")),
				Arrays.asList(
						null, StringData.fromString("\n"),
						StringData.fromString("a"), StringData.fromString("\n"),
						StringData.fromString("b"), StringData.fromString("\n")),
				Arrays.asList(
						StringData.fromString("a"), StringData.fromString(","),
						StringData.fromString("b"), StringData.fromString(","),
						null, StringData.fromString("\n"),
						StringData.fromString("c"), StringData.fromString(",")),
				Arrays.asList(
						StringData.fromString("a"), StringData.fromString(","),
						StringData.fromString("b"), StringData.fromString(","),
						null, StringData.fromString("\n"),
						StringData.fromString("c"), StringData.fromString("\n"))
		);
	}

	@Override
	protected List<StringData> getExpectedResults() {
		return Arrays.asList(
				StringData.fromString("a\nb\nc\nd\ne\nf"),
				null,
				null,
				StringData.fromString("a\nb"),
				StringData.fromString("a,b,c"),
				StringData.fromString("a\nb\nc"));
	}

	@Override
	protected AggregateFunction<StringData, ListAggWsWithRetractAccumulator> getAggregator() {
		return new ListAggWsWithRetractAggFunction();
	}

	@Override
	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod(
				"accumulate", getAccClass(), StringData.class, StringData.class);
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), StringData.class, StringData.class);
	}

	@Override
	protected Class<?> getAccClass() {
		return ListAggWsWithRetractAccumulator.class;
	}

	@Override
	protected ListAggWsWithRetractAccumulator accumulateValues(List<StringData> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<StringData, ListAggWsWithRetractAccumulator> aggregator = getAggregator();
		ListAggWsWithRetractAccumulator accumulator = getAggregator().createAccumulator();
		Method accumulateFunc = getAccumulateFunc();
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		for (int i = 0; i < values.size(); i += 2) {
			StringData value = values.get(i + 1);
			StringData delimiter = values.get(i);
			accumulateFunc.invoke(aggregator, accumulator, delimiter, value);
		}
		return accumulator;
	}

	@Override
	protected void retractValues(ListAggWsWithRetractAccumulator accumulator, List<StringData> values)
			throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		AggregateFunction<StringData, ListAggWsWithRetractAccumulator> aggregator = getAggregator();
		Method retractFunc = getRetractFunc();
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		for (int i = 0; i < values.size(); i += 2) {
			StringData value = values.get(i + 1);
			StringData delimiter = values.get(i);
			retractFunc.invoke(aggregator, accumulator, delimiter, value);
		}
	}

	@Override
	protected Tuple2<List<StringData>, List<StringData>> splitValues(List<StringData> values) {
		Preconditions.checkArgument(values.size() % 2 == 0,
				"number of values must be an integer multiple of 2.");
		int index = values.size() / 2;
		if (index % 2 != 0) {
			index -= 1;
		}
		return super.splitValues(values, index);
	}
}
