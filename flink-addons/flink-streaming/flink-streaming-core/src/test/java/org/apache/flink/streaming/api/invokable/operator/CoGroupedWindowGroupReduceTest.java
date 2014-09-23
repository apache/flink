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
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedWindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoGroupedWindowGroupReduceTest {

	public static final class MyCoGroupReduceFunction1 implements
			CoGroupReduceFunction<Character, Character, String> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unused")
		@Override
		public void reduce1(Iterable<Character> values, Collector<String> out) throws Exception {
			Integer gather = 0;
			Character ch = values.iterator().next();
			for (Character value : values) {
				gather++;
			}
			out.collect(ch + ":" + gather);
		}

		@SuppressWarnings("unused")
		@Override
		public void reduce2(Iterable<Character> values, Collector<String> out) throws Exception {
			Integer gather = 0;
			Character ch = values.iterator().next();
			for (Character value : values) {
				gather++;
			}
			out.collect(ch + ":" + gather);
		}
	}

	public static final class MyCoGroupReduceFunction2 implements
			CoGroupReduceFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce1(Iterable<Tuple2<String, Integer>> values, Collector<String> out)
				throws Exception {
			String gather = "";
			for (Tuple2<String, Integer> value : values) {
				gather += value.f0;
			}
			out.collect(gather);
		}

		@Override
		public void reduce2(Iterable<Tuple2<Integer, Integer>> values, Collector<String> out)
				throws Exception {
			Integer gather = 0;
			for (Tuple2<Integer, Integer> value : values) {
				gather += value.f0;
			}
			out.collect(gather.toString());
		}
	}

	public static final class MyTimeStamp<T> implements TimeStamp<T> {
		private static final long serialVersionUID = 1L;

		private Iterator<Long> timestamps;
		private long start;

		public MyTimeStamp(List<Long> timestamps) {
			this.timestamps = timestamps.iterator();
			this.start = timestamps.get(0);
		}

		@Override
		public long getTimestamp(T value) {
			long ts = timestamps.next();
			return ts;
		}

		@Override
		public long getStartTime() {
			return start;
		}
	}

	@Test
	public void coGroupedWindowGroupReduceTest1() {

		List<Character> inputs1 = new ArrayList<Character>();
		inputs1.add('a');
		inputs1.add('b');
		inputs1.add('c');
		inputs1.add('a');
		inputs1.add('a');
		inputs1.add('c');
		inputs1.add('b');
		inputs1.add('c');
		inputs1.add('a');
		inputs1.add('a');
		inputs1.add('x');

		List<Character> inputs2 = new ArrayList<Character>();
		inputs2.add('a');
		inputs2.add('d');
		inputs2.add('d');
		inputs2.add('e');
		inputs2.add('d');
		inputs2.add('e');
		inputs2.add('e');
		inputs2.add('a');
		inputs2.add('a');
		inputs2.add('x');

		List<Long> timestamps1 = Arrays.asList(0L, 1L, 1L, 1L, 2L, 2L, 2L, 3L, 8L, 10L, 11L);

		List<Long> timestamps2 = Arrays.asList(0L, 5L, 5L, 6L, 6L, 7L, 7L, 8L, 8L, 10L);

		CoGroupedWindowGroupReduceInvokable<Character, Character, String> invokable = new CoGroupedWindowGroupReduceInvokable<Character, Character, String>(
				new MyCoGroupReduceFunction1(), 5L, 5L, 3L, 5L, 0, 0, new MyTimeStamp<Character>(
						timestamps1), new MyTimeStamp<Character>(timestamps2));

		List<String> expected = new ArrayList<String>();
		expected.add("a:3");
		expected.add("b:2");
		expected.add("c:3");
		expected.add("c:1");
		expected.add("a:2");
		expected.add("a:1");
		expected.add("a:2");
		expected.add("d:3");
		expected.add("e:3");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

	@Test
	public void coGroupedWindowGroupReduceTest2() {

		List<Tuple2<String, Integer>> inputs1 = new ArrayList<Tuple2<String, Integer>>();
		inputs1.add(new Tuple2<String, Integer>("a", 1));
		inputs1.add(new Tuple2<String, Integer>("b", 1));
		inputs1.add(new Tuple2<String, Integer>("c", 0));
		inputs1.add(new Tuple2<String, Integer>("d", 0));
		inputs1.add(new Tuple2<String, Integer>("e", 1));
		inputs1.add(new Tuple2<String, Integer>("f", 1));
		inputs1.add(new Tuple2<String, Integer>("g", 0));
		inputs1.add(new Tuple2<String, Integer>("h", 0));
		inputs1.add(new Tuple2<String, Integer>("i", 1));
		inputs1.add(new Tuple2<String, Integer>("j", 1));

		List<Tuple2<Integer, Integer>> inputs2 = new ArrayList<Tuple2<Integer, Integer>>();
		inputs2.add(new Tuple2<Integer, Integer>(1, 1));
		inputs2.add(new Tuple2<Integer, Integer>(2, 2));
		inputs2.add(new Tuple2<Integer, Integer>(3, 1));
		inputs2.add(new Tuple2<Integer, Integer>(4, 2));
		inputs2.add(new Tuple2<Integer, Integer>(5, 1));
		inputs2.add(new Tuple2<Integer, Integer>(6, 2));
		inputs2.add(new Tuple2<Integer, Integer>(7, 1));
		inputs2.add(new Tuple2<Integer, Integer>(8, 2));
		inputs2.add(new Tuple2<Integer, Integer>(9, 1));
		inputs2.add(new Tuple2<Integer, Integer>(10, 2));

		List<Long> timestamps1 = Arrays.asList(0L, 1L, 1L, 1L, 2L, 2L, 2L, 3L, 4L, 7L);

		List<Long> timestamps2 = Arrays.asList(0L, 5L, 5L, 6L, 6L, 7L, 7L, 8L, 8L, 10L);

		CoGroupedWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<Integer, Integer>, String> invokable = new CoGroupedWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<Integer, Integer>, String>(
				new MyCoGroupReduceFunction2(), 2L, 4L, 2L, 2L, 1, 1,
				new MyTimeStamp<Tuple2<String, Integer>>(timestamps1),
				new MyTimeStamp<Tuple2<Integer, Integer>>(timestamps2));

		List<String> expected = new ArrayList<String>();
		expected.add("ab");
		expected.add("cd");
		expected.add("ef");
		expected.add("gh");
		expected.add("i");
		expected.add("1");
		expected.add("3");
		expected.add("2");
		expected.add("15");
		expected.add("12");
		expected.add("21");
		expected.add("18");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}
}
