/**
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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.function.aggregation.StreamingMaxAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.StreamingMinAggregationFunction;
import org.apache.flink.streaming.api.function.aggregation.StreamingSumAggregationFunction;
import org.apache.flink.streaming.api.invokable.operator.GroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.StreamReduceInvokable;
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class AggregationFunctionTest {

	@Test
	public void groupSumIntegerTest() {
		ArrayList<Tuple2<Integer, Integer>> inputList = new ArrayList<Tuple2<Integer, Integer>>();

		List<Tuple2<Integer, Integer>> expectedSumList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedMinList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedMaxList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedGroupSumList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedGroupMinList = new ArrayList<Tuple2<Integer, Integer>>();
		List<Tuple2<Integer, Integer>> expectedGroupMaxList = new ArrayList<Tuple2<Integer, Integer>>();

		int groupedSum0 = 0;
		int groupedSum1 = 0;
		int groupedSum2 = 0;

		for (int i = 0; i < 9; i++) {
			inputList.add(new Tuple2<Integer, Integer>(i % 3, i));

			expectedSumList.add(new Tuple2<Integer, Integer>(i % 3, (i + 1) * i / 2));
			expectedMinList.add(new Tuple2<Integer, Integer>(i % 3, 0));
			expectedMaxList.add(new Tuple2<Integer, Integer>(i % 3, i));

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

		StreamingSumAggregationFunction<Tuple2<Integer, Integer>> sumFunction = new StreamingSumAggregationFunction<Tuple2<Integer, Integer>>(
				1);
		StreamingMinAggregationFunction<Tuple2<Integer, Integer>> minFunction = new StreamingMinAggregationFunction<Tuple2<Integer, Integer>>(
				1);
		StreamingMaxAggregationFunction<Tuple2<Integer, Integer>> maxFunction = new StreamingMaxAggregationFunction<Tuple2<Integer, Integer>>(
				1);

		sumFunction.setType(TypeExtractor.getForObject(new Tuple2<Integer, Integer>(0, 0)));
		minFunction.setType(TypeExtractor.getForObject(new Tuple2<Integer, Integer>(0, 0)));
		maxFunction.setType(TypeExtractor.getForObject(new Tuple2<Integer, Integer>(0, 0)));

		List<Tuple2<Integer, Integer>> sumList = MockInvokable.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(sumFunction), inputList);
		List<Tuple2<Integer, Integer>> minList = MockInvokable.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(minFunction), inputList);
		List<Tuple2<Integer, Integer>> maxList = MockInvokable.createAndExecute(
				new StreamReduceInvokable<Tuple2<Integer, Integer>>(maxFunction), inputList);

		List<Tuple2<Integer, Integer>> groupedSumList = MockInvokable.createAndExecute(
				new GroupReduceInvokable<Tuple2<Integer, Integer>>(sumFunction, 0), inputList);
		List<Tuple2<Integer, Integer>> groupedMinList = MockInvokable.createAndExecute(
				new GroupReduceInvokable<Tuple2<Integer, Integer>>(minFunction, 0), inputList);
		List<Tuple2<Integer, Integer>> groupedMaxList = MockInvokable.createAndExecute(
				new GroupReduceInvokable<Tuple2<Integer, Integer>>(maxFunction, 0), inputList);

		assertEquals(expectedSumList, sumList);
		assertEquals(expectedMinList, minList);
		assertEquals(expectedMaxList, maxList);
		assertEquals(expectedGroupSumList, groupedSumList);
		assertEquals(expectedGroupMinList, groupedMinList);
		assertEquals(expectedGroupMaxList, groupedMaxList);
	}
}
