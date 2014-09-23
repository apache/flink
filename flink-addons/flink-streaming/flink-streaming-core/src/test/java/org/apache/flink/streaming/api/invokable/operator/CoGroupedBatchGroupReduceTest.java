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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedBatchGroupReduceInvokable;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoGroupedBatchGroupReduceTest {

	public static final class MyCoGroupReduceFunction implements
			CoGroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, String>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce1(Iterable<Tuple2<Integer, Integer>> values, Collector<String> out)
				throws Exception {
			String gather = "";
			for (Tuple2<Integer, Integer> value : values) {
				gather += value.f1.toString();
			}
			out.collect(gather);
		}

		@Override
		public void reduce2(Iterable<Tuple2<Integer, String>> values, Collector<String> out)
				throws Exception {
			String gather = "";
			for (Tuple2<Integer, String> value : values) {
				gather += value.f1;
			}
			out.collect(gather);
		}
	}

	final static int KEY_POSITION1 = 0;
	final static int KEY_POSITION2 = 0;

	@Test
	public void coGroupedBatchGroupReduceTest1() {

		List<Tuple2<Integer, Integer>> inputs1 = new ArrayList<Tuple2<Integer, Integer>>();
		for (Integer i = 1; i <= 10; i++) {
			inputs1.add(new Tuple2<Integer, Integer>(i % 3, i));
		}

		List<Tuple2<Integer, String>> inputs2 = new ArrayList<Tuple2<Integer, String>>();
		for (char ch = 'a'; ch <= 'h'; ch++) {
			inputs2.add(new Tuple2<Integer, String>(((int) ch) % 3, ch + ""));
		}

		CoGroupedBatchGroupReduceInvokable<Tuple2<Integer, Integer>, Tuple2<Integer, String>, String> invokable = new CoGroupedBatchGroupReduceInvokable<Tuple2<Integer, Integer>, Tuple2<Integer, String>, String>(
				new MyCoGroupReduceFunction(), 5L, 4L, 5L, 4L, KEY_POSITION1, KEY_POSITION2);

		List<String> expected = Arrays.asList("14", "25", "3", "69", "710", "8", "ad", "b", "c",
				"eh", "f", "g");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

	@Test
	public void coGroupedBatchGroupReduceTest2() {

		List<Tuple2<Integer, Integer>> inputs1 = new ArrayList<Tuple2<Integer, Integer>>();
		for (Integer i = 1; i <= 10; i++) {
			inputs1.add(new Tuple2<Integer, Integer>(i % 3, i));
		}

		List<Tuple2<Integer, String>> inputs2 = new ArrayList<Tuple2<Integer, String>>();
		for (char ch = 'a'; ch <= 'h'; ch++) {
			inputs2.add(new Tuple2<Integer, String>(((int) ch) % 3, ch + ""));
		}

		CoGroupedBatchGroupReduceInvokable<Tuple2<Integer, Integer>, Tuple2<Integer, String>, String> invokable = new CoGroupedBatchGroupReduceInvokable<Tuple2<Integer, Integer>, Tuple2<Integer, String>, String>(
				new MyCoGroupReduceFunction(), 6L, 6L, 3L, 2L, KEY_POSITION1, KEY_POSITION2);

		List<String> expected = Arrays.asList("14", "25", "36", "47", "58", "69", "ad", "be", "cf",
				"cf", "dg", "eh");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

}
