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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.functions.aggfunctions.ListAggWithRetractAggFunction.ListAggWithRetractAccumulator;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in LISTAGG with retraction aggregate function.
 */
public final class ListAggWithRetractAggFunctionTest
	extends AggFunctionTestBase<StringData, ListAggWithRetractAccumulator> {

	@Override
	protected List<List<StringData>> getInputValueSets() {
		return Arrays.asList(
				Arrays.asList(
						StringData.fromString("a"),
						StringData.fromString("b"),
						null,
						StringData.fromString("c"),
						null,
						StringData.fromString("d"),
						StringData.fromString("e"),
						null,
						StringData.fromString("f")),
				Arrays.asList(null, null, null, null, null, null),
				Arrays.asList(null, StringData.fromString("a"))
		);
	}

	@Override
	protected List<StringData> getExpectedResults() {
		return Arrays.asList(
				StringData.fromString("a,b,c,d,e,f"),
				null,
				StringData.fromString("a"));
	}

	@Override
	protected AggregateFunction<StringData, ListAggWithRetractAccumulator> getAggregator() {
		return new ListAggWithRetractAggFunction();
	}

	@Override
	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("accumulate", getAccClass(), StringData.class);
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), StringData.class);
	}

	@Override
	protected Class<?> getAccClass() {
		return ListAggWithRetractAccumulator.class;
	}
}
