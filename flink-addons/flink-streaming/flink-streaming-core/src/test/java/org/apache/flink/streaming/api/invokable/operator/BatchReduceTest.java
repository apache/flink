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
import java.util.List;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.util.MockInvokable;
import org.junit.Test;

public class BatchReduceTest {

	@Test
	public void BatchReduceInvokableTest() {

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}
		BatchReduceInvokable<Integer> invokable = new BatchReduceInvokable<Integer>(
				new ReduceFunction<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						return value1 + value2;
					}
				}, 3, 2);

		List<Integer> expected = new ArrayList<Integer>();
		expected.add(6);
		expected.add(12);
		expected.add(18);
		expected.add(24);
		expected.add(19);
		assertEquals(expected, MockInvokable.createAndExecute(invokable, inputs));

		List<Integer> inputs2 = new ArrayList<Integer>();
		inputs2.add(1);
		inputs2.add(2);
		inputs2.add(-1);
		inputs2.add(-3);
		inputs2.add(-4);

		BatchReduceInvokable<Integer> invokable2 = new BatchReduceInvokable<Integer>(
				new ReduceFunction<Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer reduce(Integer value1, Integer value2) throws Exception {
						if (value1 <= value2) {
							return value1;
						} else {
							return value2;
						}
					}
				}, 2, 3);

		List<Integer> expected2 = new ArrayList<Integer>();
		expected2.add(1);
		expected2.add(-4);

		assertEquals(expected2, MockInvokable.createAndExecute(invokable2, inputs2));

	}

}
