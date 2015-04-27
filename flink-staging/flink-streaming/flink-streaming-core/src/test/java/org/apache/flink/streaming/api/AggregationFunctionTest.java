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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.ComparableAggregator;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.streaming.api.operators.StreamReduce;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
				new StreamReduce<Tuple2<Integer, Integer>>(sumFunction), getInputList());

		List<Tuple2<Integer, Integer>> minList = MockContext.createAndExecute(
				new StreamReduce<Tuple2<Integer, Integer>>(minFunction), getInputList());

		List<Tuple2<Integer, Integer>> maxList = MockContext.createAndExecute(
				new StreamReduce<Tuple2<Integer, Integer>>(maxFunction), getInputList());

		TypeInformation<Tuple2<Integer, Integer>> typeInfo = TypeExtractor
				.getForObject(new Tuple2<Integer, Integer>(1, 1));

		KeySelector<Tuple2<Integer, Integer>, ?> keySelector = KeySelectorUtil.getSelectorForKeys(
				new Keys.ExpressionKeys<Tuple2<Integer, Integer>>(new int[]{0}, typeInfo),
				typeInfo, new ExecutionConfig());

		List<Tuple2<Integer, Integer>> groupedSumList = MockContext.createAndExecute(
				new StreamGroupedReduce<Tuple2<Integer, Integer>>(sumFunction, keySelector),
				getInputList());

		List<Tuple2<Integer, Integer>> groupedMinList = MockContext.createAndExecute(
				new StreamGroupedReduce<Tuple2<Integer, Integer>>(minFunction, keySelector),
				getInputList());

		List<Tuple2<Integer, Integer>> groupedMaxList = MockContext.createAndExecute(
				new StreamGroupedReduce<Tuple2<Integer, Integer>>(maxFunction, keySelector),
				getInputList());

		assertEquals(expectedSumList, sumList);
		assertEquals(expectedMinList, minList);
		assertEquals(expectedMaxList, maxList);
		assertEquals(expectedGroupSumList, groupedSumList);
		assertEquals(expectedGroupMinList, groupedMinList);
		assertEquals(expectedGroupMaxList, groupedMaxList);
		assertEquals(expectedSumList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(sumFunction0), simpleInput));
		assertEquals(expectedMinList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(minFunction0), simpleInput));
		assertEquals(expectedMaxList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(maxFunction0), simpleInput));

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

	}

	@Test
	public void pojoGroupSumIntegerTest() {
		List<MyPojo> expectedSumList = new ArrayList<MyPojo>();
		List<MyPojo> expectedMinList = new ArrayList<MyPojo>();
		List<MyPojo> expectedMaxList = new ArrayList<MyPojo>();
		List<Integer> expectedSumList0 = new ArrayList<Integer>();
		List<Integer> expectedMinList0 = new ArrayList<Integer>();
		List<Integer> expectedMaxList0 = new ArrayList<Integer>();
		List<MyPojo> expectedGroupSumList = new ArrayList<MyPojo>();
		List<MyPojo> expectedGroupMinList = new ArrayList<MyPojo>();
		List<MyPojo> expectedGroupMaxList = new ArrayList<MyPojo>();

		List<Integer> simpleInput = new ArrayList<Integer>();

		int groupedSum0 = 0;
		int groupedSum1 = 0;
		int groupedSum2 = 0;

		for (int i = 0; i < 9; i++) {
			simpleInput.add(i);
			expectedSumList.add(new MyPojo(i % 3, (i + 1) * i / 2));
			expectedMinList.add(new MyPojo(i % 3, 0));
			expectedMaxList.add(new MyPojo(i % 3, i));

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

			expectedGroupSumList.add(new MyPojo(i % 3, groupedSum));
			expectedGroupMinList.add(new MyPojo(i % 3, i % 3));
			expectedGroupMaxList.add(new MyPojo(i % 3, i));
		}

		TypeInformation<MyPojo> type1 = TypeExtractor.getForObject(new MyPojo(0, 0));
		TypeInformation<Integer> type2 = TypeExtractor.getForObject(0);
		ExecutionConfig config = new ExecutionConfig();

		ReduceFunction<MyPojo> sumFunction = SumAggregator.getSumFunction("f1", type1, config);
		ReduceFunction<Integer> sumFunction0 = SumAggregator.getSumFunction(0, Integer.class, type2);
		ReduceFunction<MyPojo> minFunction = ComparableAggregator.getAggregator("f1", type1, AggregationType.MIN,
				false, config);
		ReduceFunction<Integer> minFunction0 = ComparableAggregator.getAggregator(0, type2, AggregationType.MIN);
		ReduceFunction<MyPojo> maxFunction = ComparableAggregator.getAggregator("f1", type1, AggregationType.MAX,
				false, config);
		ReduceFunction<Integer> maxFunction0 = ComparableAggregator.getAggregator(0, type2, AggregationType.MAX);

		List<MyPojo> sumList = MockContext.createAndExecute(
				new StreamReduce<MyPojo>(sumFunction), getInputPojoList());
		List<MyPojo> minList = MockContext.createAndExecute(
				new StreamReduce<MyPojo>(minFunction), getInputPojoList());
		List<MyPojo> maxList = MockContext.createAndExecute(
				new StreamReduce<MyPojo>(maxFunction), getInputPojoList());

		TypeInformation<MyPojo> typeInfo = TypeExtractor.getForObject(new MyPojo(1, 1));
		KeySelector<MyPojo, ?> keySelector = KeySelectorUtil.getSelectorForKeys(
				new Keys.ExpressionKeys<MyPojo>(new String[]{"f0"}, typeInfo),
				typeInfo, config);

		List<MyPojo> groupedSumList = MockContext.createAndExecute(
				new StreamGroupedReduce<MyPojo>(sumFunction, keySelector),
				getInputPojoList());
		List<MyPojo> groupedMinList = MockContext.createAndExecute(
				new StreamGroupedReduce<MyPojo>(minFunction, keySelector),
				getInputPojoList());
		List<MyPojo> groupedMaxList = MockContext.createAndExecute(
				new StreamGroupedReduce<MyPojo>(maxFunction, keySelector),
				getInputPojoList());

		assertEquals(expectedSumList, sumList);
		assertEquals(expectedMinList, minList);
		assertEquals(expectedMaxList, maxList);
		assertEquals(expectedGroupSumList, groupedSumList);
		assertEquals(expectedGroupMinList, groupedMinList);
		assertEquals(expectedGroupMaxList, groupedMaxList);
		assertEquals(expectedSumList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(sumFunction0), simpleInput));
		assertEquals(expectedMinList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(minFunction0), simpleInput));
		assertEquals(expectedMaxList0, MockContext.createAndExecute(
				new StreamReduce<Integer>(maxFunction0), simpleInput));
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
				new StreamReduce<Tuple2<Integer, Integer>>(maxByFunctionFirst),
				getInputList()));
		assertEquals(maxByLastExpected, MockContext.createAndExecute(
				new StreamReduce<Tuple2<Integer, Integer>>(maxByFunctionLast),
				getInputList()));
		assertEquals(minByLastExpected, MockContext.createAndExecute(
				new StreamReduce<Tuple2<Integer, Integer>>(minByFunctionLast),
				getInputList()));
		assertEquals(minByFirstExpected, MockContext.createAndExecute(
				new StreamReduce<Tuple2<Integer, Integer>>(minByFunctionFirst),
				getInputList()));
	}

	@Test
	public void pojoMinMaxByTest() {
		ExecutionConfig config = new ExecutionConfig();
		TypeInformation<MyPojo> type1 = TypeExtractor
				.getForObject(new MyPojo(0, 0));

		ReduceFunction<MyPojo> maxByFunctionFirst = ComparableAggregator
				.getAggregator("f0", type1, AggregationType.MAXBY, true, config);
		ReduceFunction<MyPojo> maxByFunctionLast = ComparableAggregator
				.getAggregator("f0", type1, AggregationType.MAXBY, false, config);

		ReduceFunction<MyPojo> minByFunctionFirst = ComparableAggregator
				.getAggregator("f0", type1, AggregationType.MINBY, true, config);
		ReduceFunction<MyPojo> minByFunctionLast = ComparableAggregator
				.getAggregator("f0", type1, AggregationType.MINBY, false, config);

		List<MyPojo> maxByFirstExpected = new ArrayList<MyPojo>();
		maxByFirstExpected.add(new MyPojo(0, 0));
		maxByFirstExpected.add(new MyPojo(1, 1));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));
		maxByFirstExpected.add(new MyPojo(2, 2));

		List<MyPojo> maxByLastExpected = new ArrayList<MyPojo>();
		maxByLastExpected.add(new MyPojo(0, 0));
		maxByLastExpected.add(new MyPojo(1, 1));
		maxByLastExpected.add(new MyPojo(2, 2));
		maxByLastExpected.add(new MyPojo(2, 2));
		maxByLastExpected.add(new MyPojo(2, 2));
		maxByLastExpected.add(new MyPojo(2, 5));
		maxByLastExpected.add(new MyPojo(2, 5));
		maxByLastExpected.add(new MyPojo(2, 5));
		maxByLastExpected.add(new MyPojo(2, 8));

		List<MyPojo> minByFirstExpected = new ArrayList<MyPojo>();
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));
		minByFirstExpected.add(new MyPojo(0, 0));

		List<MyPojo> minByLastExpected = new ArrayList<MyPojo>();
		minByLastExpected.add(new MyPojo(0, 0));
		minByLastExpected.add(new MyPojo(0, 0));
		minByLastExpected.add(new MyPojo(0, 0));
		minByLastExpected.add(new MyPojo(0, 3));
		minByLastExpected.add(new MyPojo(0, 3));
		minByLastExpected.add(new MyPojo(0, 3));
		minByLastExpected.add(new MyPojo(0, 6));
		minByLastExpected.add(new MyPojo(0, 6));
		minByLastExpected.add(new MyPojo(0, 6));

		assertEquals(maxByFirstExpected, MockContext.createAndExecute(
				new StreamReduce<MyPojo>(maxByFunctionFirst),
				getInputPojoList()));
		assertEquals(maxByLastExpected, MockContext.createAndExecute(
				new StreamReduce<MyPojo>(maxByFunctionLast),
				getInputPojoList()));
		assertEquals(minByLastExpected, MockContext.createAndExecute(
				new StreamReduce<MyPojo>(minByFunctionLast),
				getInputPojoList()));
		assertEquals(minByFirstExpected, MockContext.createAndExecute(
				new StreamReduce<MyPojo>(minByFunctionFirst),
				getInputPojoList()));
	}

	private List<Tuple2<Integer, Integer>> getInputList() {
		ArrayList<Tuple2<Integer, Integer>> inputList = new ArrayList<Tuple2<Integer, Integer>>();
		for (int i = 0; i < 9; i++) {
			inputList.add(new Tuple2<Integer, Integer>(i % 3, i));
		}
		return inputList;

	}

	private List<MyPojo> getInputPojoList() {
		ArrayList<MyPojo> inputList = new ArrayList<MyPojo>();
		for (int i = 0; i < 9; i++) {
			inputList.add(new MyPojo(i % 3, i));
		}
		return inputList;

	}

	public static class MyPojo implements Serializable {
		
		private static final long serialVersionUID = 1L;
		public int f0;
		public int f1;

		public MyPojo(int f0, int f1) {
			this.f0 = f0;
			this.f1 = f1;
		}

		public MyPojo() {
		}

		@Override
		public String toString() {
			return "POJO(" + f0 + "," + f1 + ")";
		}

		@Override
		public boolean equals(Object other) {
			if (other instanceof MyPojo) {
				return this.f0 == ((MyPojo) other).f0 && this.f1 == ((MyPojo) other).f1;
			} else {
				return false;
			}
		}

	}
}
