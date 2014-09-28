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

package org.apache.flink.streaming.api.invokable.operator;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoWindowReduceInvokable;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.junit.Test;

public class CoWindowReduceTest {

	private static class MyCoReduceFunction implements CoReduceFunction<Integer, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Integer reduce1(Integer value1, Integer value2) {
			return value1 + value2;
		}

		@Override
		public Integer reduce2(Integer value1, Integer value2) {
			return value1 + value2;
		}

		@Override
		public String map1(Integer value) {
			return value.toString();
		}

		@Override
		public String map2(Integer value) {
			return value.toString();
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

	List<Long> timestamps1 = Arrays.asList(0L, 1L, 1L, 1L, 2L, 2L, 2L, 3L, 8L, 10L);

	List<Long> timestamps2 = Arrays.asList(0L, 5L, 5L, 6L, 6L);

	@Test
	public void coWindowReduceTest() {

		List<Integer> inputs = new ArrayList<Integer>();
		for (Integer i = 1; i <= 10; i++) {
			inputs.add(i);
		}

		List<Integer> inputs2 = new ArrayList<Integer>();
		inputs2.add(1);
		inputs2.add(2);
		inputs2.add(-1);
		inputs2.add(-3);
		inputs2.add(-4);

		CoWindowReduceInvokable<Integer, Integer, String> invokable = new CoWindowReduceInvokable<Integer, Integer, String>(
				new MyCoReduceFunction(), 3L, 3L, 2L, 2L, new MyTimeStamp<Integer>(timestamps1),
				new MyTimeStamp<Integer>(timestamps2));

		List<String> expected = new ArrayList<String>();
		expected.add("28");
		expected.add("26");
		expected.add("9");
		expected.add("19");
		expected.add("1");
		expected.add("-6");

		List<String> result = MockCoInvokable.createAndExecute(invokable, inputs, inputs2);

		Collections.sort(result);
		Collections.sort(expected);
		assertEquals(expected, result);

	}

}
