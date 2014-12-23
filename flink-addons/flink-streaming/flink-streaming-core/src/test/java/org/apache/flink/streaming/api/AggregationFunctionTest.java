/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.function.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.function.aggregation.SumAggregator;
import org.apache.flink.streaming.api.invokable.operator.GroupedReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.StreamReduceInvokable;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.junit.Test;

public class AggregationFunctionTest {

	@Test
	public void groupSumIntegerTest() {

		List<Tuple2<Integer, Integer>> expectedSumList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedMinList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedMaxList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Integer> expectedSumList0 = new ArrayList<Integer>();
		List<Integer> expectedMinList0 = new ArrayList<Integer>();
		List<Integer> expectedMaxList0 = new ArrayList<Integer>();
		List<Tuple2<Integer, Integer>> expectedGroupSumList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedGroupMinList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedGroupMaxList = new ArrayList<Tuple2<Integer, Integer>>();

		List<Integer> simpleInput = new ArrayList<Integer>();

		int groupedSum0 = 0;
		int groupedSum1 = 0;
		int groupedSum2 = 0;

		for (int i = 0; i < 9; i++) {
			simpleInput.add(i);
			expectedSumList.add(new Tuple2<Integer, Integer>(i % 3, (i + 1) * i / 2));
			expectedMinList.add(new Tuple2<Integer, Integer>(i % 3, 0));
			expectedMaxList.add(new Tuple2<Integer, Integer>(i % 3, i));

			expectedSumList0.add((i + 1) * i / 2);
			expectedMaxList0.add(i);
			expectedMinList0.add(0);

			int groupedSum;
			switch (i % 3) {
			case 0:
				groupedSum = groupedSum0 += i;
				break;
			case 1:
				groupedSum = groupedSum1 += i;
				break;
			default:
				groupedSum = groupedSum2 += i;
				break;
			}

			expectedGroupSumList.add(new Tuple2<Integer, Integer>(i % 3, groupedSum));
			expectedGroupMinList.add(new Tuple2<Integer, Integer>(i % 3, i % 3));
			expectedGroupMaxList.add(new Tuple2<Integer, Integer>(i % 3, i));
		}

		TypeInformation<Tuple2<Integer, Integer>> type1 = TypeExtractor
				.getForObject(new Tuple2<Integer, Integer>(0, 0));
		TypeInformation<Integer> type2 = TypeExtractor.getForObject(2);

		ReduceFunction<Tuple2<Integer, Integer>> sumFunction = SumAggregator.getSumFunction(1,
				Integer.class, type1);
		ReduceFunction<Integer> sumFunction0 = SumAggregator
				.getSumFunction(0, Integer.class, type2);
		ReduceFunction<Tuple2<Integer, Integer>> minFunction = ComparableAggregator.getAggregator(
				1, type1, AggregationType.MIN);
		ReduceFunction<Integer> minFunction0 = ComparableAggregator.getAggregator(0, type2,
				AggregationType.MIN);
		ReduceFunction<Tuple2<Integer, Integer>> maxFunction = ComparableAggregator.getAggregator(
				1, type1, AggregationType.MAX);
		ReduceFunction<Integer> maxFunction0 = ComparableAggregator.getAggregator(0, type2,
				AggregationType.MAX);
		List<Tuple2<Integer, Integer>> sumList = MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(sumFunction), getInputList());

		List<Tuple2<Integer, Integer>> minList = MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minFunction), getInputList());

		List<Tuple2<Integer, Integer>> maxList = MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxFunction), getInputList());

		TypeInformation<Tuple2<Integer, Integer>> typeInfo = TypeExtractor
				.getForObject(new Tuple2<Integer, Integer>(1, 1));

		KeySelector<Tuple2<Integer, Integer>, ?> keySelector = KeySelectorUtil.getSelectorForKeys(
				new Keys.ExpressionKeys<Tuple2<Integer, Integer>>(new int[] { 0 }, typeInfo),
				typeInfo);

		List<Tuple2<Integer, Integer>> groupedSumList = MockContext.createAndExecute(
				new GroupedReduceInvokable<Tuple2<Integer, Integer>>(sumFunction, keySelector),
				getInputList());

		List<Tuple2<Integer, Integer>> groupedMinList = MockContext.createAndExecute(
				new GroupedReduceInvokable<Tuple2<Integer, Integer>>(minFunction, keySelector),
				getInputList());

		List<Tuple2<Integer, Integer>> groupedMaxList = MockContext.createAndExecute(
				new GroupedReduceInvokable<Tuple2<Integer, Integer>>(maxFunction, keySelector),
				getInputList());

		assertEquals(expectedSumList, sumList);
		assertEquals(expectedMinList, minList);
		assertEquals(expectedMaxList, maxList);
		assertEquals(expectedGroupSumList, groupedSumList);
		assertEquals(expectedGroupMinList, groupedMinList);
		assertEquals(expectedGroupMaxList, groupedMaxList);
		assertEquals(expectedSumList0, MockContext.createAndExecute(
				new StreamReduceInvokable<Integer>(sumFunction0), simpleInput));
		assertEquals(expectedMinList0, MockContext.createAndExecute(
				new StreamReduceInvokable<Integer>(minFunction0), simpleInput));
		assertEquals(expectedMaxList0, MockContext.createAndExecute(
				new StreamReduceInvokable<Integer>(maxFunction0), simpleInput));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		try {
			env.generateSequence(1, 100).min(1);
			fail();
		} catch (Exception e) {
			// Nothing to do here
		}
		try {
			env.generateSequence(1, 100).min(2);
			fail();
		} catch (Exception e) {
			// Nothing to do here
		}
		try {
			env.generateSequence(1, 100).min(3);
			fail();
		} catch (Exception e) {
			// Nothing to do here
		}

		ReduceFunction<Tuple2<Integer, Integer>> maxByFunctionFirst = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MAXBY, true);
		ReduceFunction<Tuple2<Integer, Integer>> maxByFunctionLast = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MAXBY, false);

		ReduceFunction<Tuple2<Integer, Integer>> minByFunctionFirst = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MINBY, true);
		ReduceFunction<Tuple2<Integer, Integer>> minByFunctionLast = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MINBY, false);

		List<Tuple2<Integer, Integer>> maxByFirstExpected = new ArrayList<Tuple2<Integer, Integer>>();
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(1, 1));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));

		List<Tuple2<Integer, Integer>> maxByLastExpected = new ArrayList<Tuple2<Integer, Integer>>();
		maxByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(1, 1));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 8));

		List<Tuple2<Integer, Integer>> minByFirstExpected = new ArrayList<Tuple2<Integer, Integer>>();
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));

		List<Tuple2<Integer, Integer>> minByLastExpected = new ArrayList<Tuple2<Integer, Integer>>();
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));

		assertEquals(maxByFirstExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxByFunctionFirst),
				getInputList()));
		assertEquals(maxByLastExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxByFunctionLast),
				getInputList()));
		assertEquals(minByLastExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minByFunctionLast),
				getInputList()));
		assertEquals(minByFirstExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minByFunctionFirst),
				getInputList()));

	}

	@Test
	public void minMaxByTest() {
		TypeInformation<Tuple2<Integer, Integer>> type1 = TypeExtractor
				.getForObject(new Tuple2<Integer, Integer>(0, 0));

		ReduceFunction<Tuple2<Integer, Integer>> maxByFunctionFirst = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MAXBY, true);
		ReduceFunction<Tuple2<Integer, Integer>> maxByFunctionLast = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MAXBY, false);

		ReduceFunction<Tuple2<Integer, Integer>> minByFunctionFirst = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MINBY, true);
		ReduceFunction<Tuple2<Integer, Integer>> minByFunctionLast = ComparableAggregator
				.getAggregator(0, type1, AggregationType.MINBY, false);

		List<Tuple2<Integer, Integer>> maxByFirstExpected = new ArrayList<Tuple2<Integer, Integer>>();
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(1, 1));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByFirstExpected.add(new Tuple2<Integer, Integer>(2, 2));

		List<Tuple2<Integer, Integer>> maxByLastExpected = new ArrayList<Tuple2<Integer, Integer>>();
		maxByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(1, 1));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 2));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 5));
		maxByLastExpected.add(new Tuple2<Integer, Integer>(2, 8));

		List<Tuple2<Integer, Integer>> minByFirstExpected = new ArrayList<Tuple2<Integer, Integer>>();
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByFirstExpected.add(new Tuple2<Integer, Integer>(0, 0));

		List<Tuple2<Integer, Integer>> minByLastExpected = new ArrayList<Tuple2<Integer, Integer>>();
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 0));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 3));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));
		minByLastExpected.add(new Tuple2<Integer, Integer>(0, 6));

		assertEquals(maxByFirstExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxByFunctionFirst),
				getInputList()));
		assertEquals(maxByLastExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxByFunctionLast),
				getInputList()));
		assertEquals(minByLastExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minByFunctionLast),
				getInputList()));
		assertEquals(minByFirstExpected, MockContext.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minByFunctionFirst),
				getInputList()));
	}

	private List<Tuple2<Integer, Integer>> getInputList() {
		ArrayList<Tuple2<Integer, Integer>> inputList = new ArrayList<Tuple2<Integer, Integer>>();
		for (int i = 0; i < 9; i++) {
			inputList.add(new Tuple2<Integer, Integer>(i % 3, i));
		}
		return inputList;

	}
}
