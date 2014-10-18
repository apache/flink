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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedBatchReduceInvokable;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.junit.Test;

public class CoGroupedBatchReduceTest {

	private static class MyCoReduceFunction implements
			CoReduceFunction<Tuple2<String, Integer>, Tuple2<String, String>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> reduce1(Tuple2<String, Integer> value1,
				Tuple2<String, Integer> value2) {
			return new Tuple2<String, Integer>("a", value1.f1 + value2.f1);
		}

		@Override
		public Tuple2<String, String> reduce2(Tuple2<String, String> value1,
				Tuple2<String, String> value2) {
			return new Tuple2<String, String>("a", value1.f1 + value2.f1);
		}

		@Override
		public String map1(Tuple2<String, Integer> value) {
			return value.f1.toString();
		}

		@Override
		public String map2(Tuple2<String, String> value) {
			return value.f1;
		}
	}

	@Test
	public void coGroupedBatchReduceTest1() {

		List<Tuple2<String, Integer>> inputs1 = new ArrayList<Tuple2<String, Integer>>();
		inputs1.add(new Tuple2<String, Integer>("a", 1));
		inputs1.add(new Tuple2<String, Integer>("a", 2));
		inputs1.add(new Tuple2<String, Integer>("a", 3));
		inputs1.add(new Tuple2<String, Integer>("a", 4));
		inputs1.add(new Tuple2<String, Integer>("a", 5));
		inputs1.add(new Tuple2<String, Integer>("b", 6));
		inputs1.add(new Tuple2<String, Integer>("a", 7));
		inputs1.add(new Tuple2<String, Integer>("b", 8));
		inputs1.add(new Tuple2<String, Integer>("b", 9));
		inputs1.add(new Tuple2<String, Integer>("b", 10));

		List<Tuple2<String, String>> inputs2 = new ArrayList<Tuple2<String, String>>();
		inputs2.add(new Tuple2<String, String>("1", "a"));
		inputs2.add(new Tuple2<String, String>("2", "b"));
		inputs2.add(new Tuple2<String, String>("1", "c"));
		inputs2.add(new Tuple2<String, String>("2", "d"));
		inputs2.add(new Tuple2<String, String>("1", "e"));
		inputs2.add(new Tuple2<String, String>("2", "f"));
		inputs2.add(new Tuple2<String, String>("1", "g"));
		inputs2.add(new Tuple2<String, String>("2", "h"));
		inputs2.add(new Tuple2<String, String>("1", "i"));

		List<String> expected = new ArrayList<String>();
		expected.add("10");
		expected.add("12");
		expected.add("33");
		expected.add("ace");
		expected.add("gi");
		expected.add("bdf");
		expected.add("h");

		CoGroupedBatchReduceInvokable<Tuple2<String, Integer>, Tuple2<String, String>, String> invokable = new CoGroupedBatchReduceInvokable<Tuple2<String, Integer>, Tuple2<String, String>, String>(
				new MyCoReduceFunction(), 4L, 3L, 4L, 3L, 0, 0);

		List<String> result = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);

		Collections.sort(result);
		Collections.sort(expected);
		assertEquals(expected, result);
	}

	@Test
	public void coGroupedBatchReduceTest2() {

		List<Tuple2<String, Integer>> inputs1 = new ArrayList<Tuple2<String, Integer>>();
		inputs1.add(new Tuple2<String, Integer>("a", 1));
		inputs1.add(new Tuple2<String, Integer>("a", 2));
		inputs1.add(new Tuple2<String, Integer>("a", 3));
		inputs1.add(new Tuple2<String, Integer>("a", 4));
		inputs1.add(new Tuple2<String, Integer>("a", 5));
		inputs1.add(new Tuple2<String, Integer>("b", 6));
		inputs1.add(new Tuple2<String, Integer>("a", 7));
		inputs1.add(new Tuple2<String, Integer>("b", 8));
		inputs1.add(new Tuple2<String, Integer>("b", 9));
		inputs1.add(new Tuple2<String, Integer>("b", 10));

		List<Tuple2<String, String>> inputs2 = new ArrayList<Tuple2<String, String>>();
		inputs2.add(new Tuple2<String, String>("1", "a"));
		inputs2.add(new Tuple2<String, String>("2", "b"));
		inputs2.add(new Tuple2<String, String>("1", "c"));
		inputs2.add(new Tuple2<String, String>("2", "d"));
		inputs2.add(new Tuple2<String, String>("1", "e"));
		inputs2.add(new Tuple2<String, String>("2", "f"));
		inputs2.add(new Tuple2<String, String>("1", "g"));
		inputs2.add(new Tuple2<String, String>("2", "h"));
		inputs2.add(new Tuple2<String, String>("1", "i"));

		List<String> expected = new ArrayList<String>();
		expected.add("10");
		expected.add("19");
		expected.add("33");
		expected.add("ace");
		expected.add("egi");
		expected.add("bdf");
		expected.add("fh");

		CoGroupedBatchReduceInvokable<Tuple2<String, Integer>, Tuple2<String, String>, String> invokable = new CoGroupedBatchReduceInvokable<Tuple2<String, Integer>, Tuple2<String, String>, String>(
				new MyCoReduceFunction(), 4L, 3L, 2L, 2L, 0, 0);

		List<String> result = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);

		Collections.sort(result);
		Collections.sort(expected);
		assertEquals(expected, result);
	}

}
