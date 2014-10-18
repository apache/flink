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
import java.util.HashSet;
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class WindowReduceInvokableTest {

	@Test
	public void windowReduceTest() {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(2);
		inputs.add(2);
		inputs.add(3);
		inputs.add(4);
		inputs.add(5);
		inputs.add(10);
		inputs.add(11);
		inputs.add(11);
		// 1,2,3,4-3,4,5,6-5,6,7,8-7,8,9,10-9,10,11
		// 12-12-5-10-32

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(12);
		expected.add(12);
		expected.add(5);
		expected.add(10);
		expected.add(32);

		WindowReduceInvokable<Integer> invokable = new WindowReduceInvokable<Integer>(
				new ReduceFunction<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						return value1 + value2;
					}
				}, 4, 2, new TimeStamp<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Integer value) {
						return value;
					}

					@Override
					public long getStartTime() {
						return 1;
					}
				});

		assertEquals(expected, MockInvokable.createAndExecute(invokable, inputs));

		List<Tuple2<String, Integer>> inputs2 = new ArrayList<Tuple2<String, Integer>>();
		inputs2.add(new Tuple2<String, Integer>("a", 1));
		inputs2.add(new Tuple2<String, Integer>("a", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 2));
		inputs2.add(new Tuple2<String, Integer>("b", 5));
		inputs2.add(new Tuple2<String, Integer>("a", 7));
		inputs2.add(new Tuple2<String, Integer>("b", 9));
		inputs2.add(new Tuple2<String, Integer>("b", 10));

		List<Tuple2<String, Integer>> expected2 = new ArrayList<Tuple2<String, Integer>>();
		expected2.add(new Tuple2<String, Integer>("a", 3));
		expected2.add(new Tuple2<String, Integer>("b", 4));
		expected2.add(new Tuple2<String, Integer>("b", 5));
		expected2.add(new Tuple2<String, Integer>("a", 7));
		expected2.add(new Tuple2<String, Integer>("b", 10));

		GroupedWindowReduceInvokable<Tuple2<String, Integer>> invokable2 = new GroupedWindowReduceInvokable<Tuple2<String, Integer>>(
				new ReduceFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
						return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
					}
				}, 2, 3, 0, new TimeStamp<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public long getTimestamp(Tuple2<String, Integer> value) {
						return value.f1;
					}

					@Override
					public long getStartTime() {
						return 1;
					}
				});

		List<Tuple2<String, Integer>> actual2 = MockInvokable.createAndExecute(invokable2, inputs2);
		assertEquals(new HashSet<Tuple2<String, Integer>>(expected2),
				new HashSet<Tuple2<String, Integer>>(actual2));
		assertEquals(expected2.size(), actual2.size());

	}

}
