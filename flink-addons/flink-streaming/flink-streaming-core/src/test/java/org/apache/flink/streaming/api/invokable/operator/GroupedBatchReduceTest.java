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
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class GroupedBatchReduceTest {

	@Test
	public void BatchReduceInvokableTest() {

		List<Integer> inputs = new ArrayList<Integer>();
		inputs.add(1);
		inputs.add(1);
		inputs.add(5);
		inputs.add(5);
		inputs.add(5);
		inputs.add(1);
		inputs.add(1);
		inputs.add(5);
		inputs.add(1);
		inputs.add(5);
		
		List<Integer> expected = new ArrayList<Integer>();
		expected.add(15);
		expected.add(3);
		expected.add(3);
		expected.add(15);


		GroupedBatchReduceInvokable<Integer> invokable = new GroupedBatchReduceInvokable<Integer>(
				new ReduceFunction<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						return value1 + value2;
					}
				}, 3, 2, 0);
		
		List<Integer> actual = MockInvokable.createAndExecute(invokable, inputs);
		assertEquals(new HashSet<Integer>(expected), new HashSet<Integer>(actual));
		assertEquals(expected.size(), actual.size());

		List<Tuple2<Integer, String>> inputs2 = new ArrayList<Tuple2<Integer, String>>();
		inputs2.add(new Tuple2<Integer, String>(1, "a"));
		inputs2.add(new Tuple2<Integer, String>(0, "b"));
		inputs2.add(new Tuple2<Integer, String>(2, "a"));
		inputs2.add(new Tuple2<Integer, String>(-1, "a"));
		inputs2.add(new Tuple2<Integer, String>(-2, "a"));
		inputs2.add(new Tuple2<Integer, String>(10, "a"));
		inputs2.add(new Tuple2<Integer, String>(2, "b"));
		inputs2.add(new Tuple2<Integer, String>(1, "a"));
		
		List<Tuple2<Integer, String>> expected2 = new ArrayList<Tuple2<Integer, String>>();
		expected2.add(new Tuple2<Integer, String>(-1, "a"));
		expected2.add(new Tuple2<Integer, String>(-2, "a"));
		expected2.add(new Tuple2<Integer, String>(0, "b"));

		GroupedBatchReduceInvokable<Tuple2<Integer, String>> invokable2 = new GroupedBatchReduceInvokable<Tuple2<Integer, String>>(
				new ReduceFunction<Tuple2<Integer, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1,
							Tuple2<Integer, String> value2) throws Exception {
						if (value1.f0 <= value2.f0) {
							return value1;
						} else {
							return value2;
						}
					}
				}, 3, 3, 1);

		

		List<Tuple2<Integer, String>> actual2 = MockInvokable.createAndExecute(invokable2, inputs2);
		
		assertEquals(new HashSet<Tuple2<Integer, String>>(expected2),
				new HashSet<Tuple2<Integer, String>>(actual2));
		assertEquals(expected2.size(), actual2.size());
	}

}
