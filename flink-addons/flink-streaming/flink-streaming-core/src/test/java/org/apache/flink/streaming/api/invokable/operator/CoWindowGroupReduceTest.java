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
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoWindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoWindowGroupReduceTest {

	
	public static final class MyCoGroupReduceFunction1 implements
			CoGroupReduceFunction<Long, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce1(Iterable<Long> values, Collector<String> out) throws Exception {
			Long gather = 0L;
			for (Long value : values) {
				gather += value;
			}
			out.collect(gather.toString());
		}

		@SuppressWarnings("unused")
		@Override
		public void reduce2(Iterable<Integer> values, Collector<String> out) throws Exception {
			Integer gather = 0;
			for (Integer value : values) {
				gather++;
			}
			out.collect(gather.toString());
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

	public static final class MyTimeStamp1 implements TimeStamp<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Long value) {
			return value;
		}

		@Override
		public long getStartTime() {
			return 0L;
		}
	}

	public static final class MyTimeStamp2 implements TimeStamp<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Integer value) {
			return value.longValue();
		}

		@Override
		public long getStartTime() {
			return 0L;
		}
	}

	public static final class MyTimeStamp3 implements TimeStamp<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Tuple2<String, Integer> value) {
			return value.f1.longValue();
		}

		@Override
		public long getStartTime() {
			return 0L;
		}
	}

	public static final class MyTimeStamp4 implements TimeStamp<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long getTimestamp(Tuple2<Integer, Integer> value) {
			return value.f1.longValue();
		}

		@Override
		public long getStartTime() {
			return 0L;
		}
	}

	@Test
	public void coWindowGroupReduceTest1() {

		List<Long> inputs1 = new ArrayList<Long>();
		inputs1.add(0L);
		inputs1.add(2L);
		inputs1.add(2L);
		inputs1.add(3L);
		inputs1.add(4L);
		inputs1.add(5L);
		inputs1.add(6L);
		inputs1.add(6L);
		inputs1.add(6L);
		inputs1.add(8L);
		inputs1.add(14L);
		inputs1.add(15L);
		inputs1.add(15L);

		List<Integer> inputs2 = new ArrayList<Integer>();
		inputs2.add(0);
		inputs2.add(0);
		inputs2.add(5);
		inputs2.add(7);
		inputs2.add(7);
		inputs2.add(7);
		inputs2.add(8);
		inputs2.add(8);
		inputs2.add(8);
		inputs2.add(14);
		inputs2.add(14);
		inputs2.add(15);
		inputs2.add(16);

		CoWindowGroupReduceInvokable<Long, Integer, String> invokable = new CoWindowGroupReduceInvokable<Long, Integer, String>(
				new MyCoGroupReduceFunction1(), 3L, 4L, 2L, 2L, new MyTimeStamp1(),
				new MyTimeStamp2());

		List<String> expected = new ArrayList<String>();
		expected.add("4");
		expected.add("11");
		expected.add("27");
		expected.add("26");
		expected.add("8");
		expected.add("0");
		expected.add("14");
		expected.add("2");
		expected.add("1");
		expected.add("4");
		expected.add("6");
		expected.add("3");
		expected.add("0");
		expected.add("3");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

	@Test
	public void coWindowGroupReduceTest2() {

		List<Tuple2<String, Integer>> inputs1 = new ArrayList<Tuple2<String, Integer>>();
		inputs1.add(new Tuple2<String, Integer>("I", 1));
		inputs1.add(new Tuple2<String, Integer>("t", 2));
		inputs1.add(new Tuple2<String, Integer>("i", 4));
		inputs1.add(new Tuple2<String, Integer>("s", 5));
		inputs1.add(new Tuple2<String, Integer>("a", 7));
		inputs1.add(new Tuple2<String, Integer>("l", 7));
		inputs1.add(new Tuple2<String, Integer>("l", 8));
		inputs1.add(new Tuple2<String, Integer>("o", 10));
		inputs1.add(new Tuple2<String, Integer>("k", 11));
		inputs1.add(new Tuple2<String, Integer>("a", 11));
		inputs1.add(new Tuple2<String, Integer>("y", 11));
		inputs1.add(new Tuple2<String, Integer>("!", 11));
		inputs1.add(new Tuple2<String, Integer>(" ", 12));

		List<Tuple2<Integer, Integer>> inputs2 = new ArrayList<Tuple2<Integer, Integer>>();
		inputs2.add(new Tuple2<Integer, Integer>(10, 1));
		inputs2.add(new Tuple2<Integer, Integer>(10, 2));
		inputs2.add(new Tuple2<Integer, Integer>(20, 2));
		inputs2.add(new Tuple2<Integer, Integer>(30, 2));
		inputs2.add(new Tuple2<Integer, Integer>(10, 3));
		inputs2.add(new Tuple2<Integer, Integer>(30, 4));
		inputs2.add(new Tuple2<Integer, Integer>(40, 5));
		inputs2.add(new Tuple2<Integer, Integer>(30, 6));
		inputs2.add(new Tuple2<Integer, Integer>(20, 7));
		inputs2.add(new Tuple2<Integer, Integer>(20, 7));
		inputs2.add(new Tuple2<Integer, Integer>(10, 7));
		inputs2.add(new Tuple2<Integer, Integer>(10, 8));
		inputs2.add(new Tuple2<Integer, Integer>(30, 9));
		inputs2.add(new Tuple2<Integer, Integer>(30, 10));

		CoWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<Integer, Integer>, String> invokable = new CoWindowGroupReduceInvokable<Tuple2<String, Integer>, Tuple2<Integer, Integer>, String>(
				new MyCoGroupReduceFunction2(), 3L, 3L, 3L, 2L, new MyTimeStamp3(),
				new MyTimeStamp4());

		List<String> expected = new ArrayList<String>();
		expected.add("It");
		expected.add("is");
		expected.add("all");
		expected.add("okay!");
		expected.add("70");
		expected.add("100");
		expected.add("100");
		expected.add("90");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}
}
