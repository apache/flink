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

package org.apache.flink.table.functions.aggfunctions;

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.aggfunctions.ConcatWithRetractAggFunction.ConcatWithRetractAccumulator;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for built-in concat with retraction aggregate function.
 */
public class ConcatWithRetractAggFunctionTest
	extends AggFunctionTestBase<BinaryString, ConcatWithRetractAccumulator> {

	@Override
	protected List<List<BinaryString>> getInputValueSets() {
		return Arrays.asList(
				Arrays.asList(
						BinaryString.fromString("a"),
						BinaryString.fromString("b"),
						null,
						BinaryString.fromString("c"),
						null,
						BinaryString.fromString("d"),
						BinaryString.fromString("e"),
						null,
						BinaryString.fromString("f")),
				Arrays.asList(null, null, null, null, null, null),
				Arrays.asList(null, BinaryString.fromString("a"))
		);
	}

	@Override
	protected List<BinaryString> getExpectedResults() {
		return Arrays.asList(
				BinaryString.fromString("a\nb\nc\nd\ne\nf"),
				null,
				BinaryString.fromString("a"));
	}

	@Override
	protected AggregateFunction<BinaryString, ConcatWithRetractAccumulator> getAggregator() {
		return new ConcatWithRetractAggFunction();
	}

	@Override
	protected Method getAccumulateFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("accumulate", getAccClass(), BinaryString.class);
	}

	@Override
	protected Method getRetractFunc() throws NoSuchMethodException {
		return getAggregator().getClass().getMethod("retract", getAccClass(), BinaryString.class);
	}

	@Override
	protected Class<?> getAccClass() {
		return ConcatWithRetractAccumulator.class;
	}
}
