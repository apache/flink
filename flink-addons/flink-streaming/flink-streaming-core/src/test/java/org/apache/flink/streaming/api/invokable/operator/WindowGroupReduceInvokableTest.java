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
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
import org.apache.flink.streaming.util.MockInvokable;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

public class WindowGroupReduceInvokableTest {

	public static final class MySlidingWindowReduce implements GroupReduceFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Integer> values, Collector<String> out) throws Exception {
			for (Integer value : values) {
				out.collect(value.toString());
			}
			out.collect(EOW);
		}
	}

	public static final class MyTimestamp implements TimeStamp<Integer> {
		private static final long serialVersionUID = 1L;

		private Iterator<Long> timestamps;
		private long start;

		public MyTimestamp(List<Long> timestamps) {
			this.timestamps = timestamps.iterator();
			this.start = timestamps.get(0);
		}

		@Override
		public long getTimestamp(Integer value) {
			long ts = timestamps.next();
			return ts;
		}

		@Override
		public long getStartTime() {
			return start;
		}
	}

	private final static String EOW = "|";

	private static List<WindowGroupReduceInvokable<Integer, String>> invokables = new ArrayList<WindowGroupReduceInvokable<Integer, String>>();
	private static List<List<String>> expectedResults = new ArrayList<List<String>>();

	@Before
	public void before() {
		long windowSize = 3;
		long slideSize = 2;
		List<Long> timestamps = Arrays.asList(101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L, 109L,
				110L);
		expectedResults.add(Arrays.asList("1", "2", "3", EOW, "3", "4", "5", EOW, "5", "6", "7",
				EOW, "7", "8", "9", EOW, "9", "10", EOW));
		invokables.add(new WindowGroupReduceInvokable<Integer, String>(new MySlidingWindowReduce(),
				windowSize, slideSize, new MyTimestamp(timestamps)));

		windowSize = 10;
		slideSize = 5;
		timestamps = Arrays.asList(101L, 103L, 121L, 122L, 123L, 124L, 180L, 181L, 185L, 190L);
		expectedResults.add(Arrays.asList("1", "2", EOW, "3", "4", "5", "6", EOW, "3",
				"4", "5", "6", EOW, "7", EOW, "7",
				"8", "9", EOW, "8", "9", "10", EOW));
		invokables.add(new WindowGroupReduceInvokable<Integer, String>(new MySlidingWindowReduce(),
				windowSize, slideSize, new MyTimestamp(timestamps)));

		windowSize = 10;
		slideSize = 4;
		timestamps = Arrays.asList(101L, 103L, 110L, 112L, 113L, 114L, 120L, 121L, 125L, 130L);
		expectedResults.add(Arrays.asList("1", "2","3" ,EOW, "3", "4", "5","6", EOW, "3", "4", "5", "6",
				EOW, "5", "6", "7", "8", EOW, "7", "8", "9", EOW, "8","9",
				"10", EOW));
		invokables.add(new WindowGroupReduceInvokable<Integer, String>(new MySlidingWindowReduce(),
				windowSize, slideSize, new MyTimestamp(timestamps)));
	}

	@Test
	public void slidingBatchReduceTest() {
		List<List<String>> actualResults = new ArrayList<List<String>>();

		for (WindowGroupReduceInvokable<Integer, String> invokable : invokables) {
			List<String> result = MockInvokable.createAndExecute(invokable,
					Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
			actualResults.add(result);
		}

		Iterator<List<String>> actualResult = actualResults.iterator();

		for (List<String> expectedResult : expectedResults) {
			assertEquals(expectedResult, actualResult.next());
		}

	}

}
