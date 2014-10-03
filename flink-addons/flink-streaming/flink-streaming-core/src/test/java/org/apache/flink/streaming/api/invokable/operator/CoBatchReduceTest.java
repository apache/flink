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
import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoBatchReduceInvokable;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.junit.Test;

public class CoBatchReduceTest {

	private static class MyCoReduceFunction implements CoReduceFunction<Integer, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce1(Integer value1, Integer value2) {
			return value1 + value2;
		}

		@Override
		public String reduce2(String value1, String value2) {
			return value1 + value2;
		}

		@Override
		public String map1(Integer value) {
			return value.toString();
		}

		@Override
		public String map2(String value) {
			return value;
		}
	}

	@Test
	public void coBatchReduceTest1() {

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}

		List<String> inputs2 = new ArrayList<String>();
		inputs2.add("a");
		inputs2.add("b");
		inputs2.add("c");
		inputs2.add("d");
		inputs2.add("e");
		inputs2.add("f");
		inputs2.add("g");
		inputs2.add("h");
		inputs2.add("i");

		CoBatchReduceInvokable<Integer, String, String> invokable = new CoBatchReduceInvokable<Integer, String, String>(
				new MyCoReduceFunction(), 4L, 3L, 4L, 3L);

		List<String> expected = new ArrayList<String>();
		expected.add("10");
		expected.add("26");
		expected.add("19");
		expected.add("abc");
		expected.add("def");
		expected.add("ghi");

		List<String> result = MockCoInvokable.createAndExecute(invokable, inputs, inputs2);

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);

	}

	@Test
	public void coBatchReduceTest2() {

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}

		List<String> inputs2 = new ArrayList<String>();
		inputs2.add("a");
		inputs2.add("b");
		inputs2.add("c");
		inputs2.add("d");
		inputs2.add("e");
		inputs2.add("f");
		inputs2.add("g");
		inputs2.add("h");
		inputs2.add("i");

		CoBatchReduceInvokable<Integer, String, String> invokable = new CoBatchReduceInvokable<Integer, String, String>(
				new MyCoReduceFunction(), 4L, 3L, 2L, 2L);

		List<String> expected = new ArrayList<String>();
		expected.add("10");
		expected.add("18");
		expected.add("26");
		expected.add("34");
		expected.add("abc");
		expected.add("cde");
		expected.add("efg");
		expected.add("ghi");

		List<String> result = MockCoInvokable.createAndExecute(invokable, inputs, inputs2);

		Collections.sort(result);
		Collections.sort(expected);

		assertEquals(expected, result);

	}

}
