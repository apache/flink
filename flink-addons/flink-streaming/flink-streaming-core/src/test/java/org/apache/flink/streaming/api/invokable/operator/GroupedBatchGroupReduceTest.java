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

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.MockInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class GroupedBatchGroupReduceTest {

	public static final class MySlidingBatchReduce1 implements GroupReduceFunction<Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Integer> values, Collector<String> out) throws Exception {
			for (Integer value : values) {
				out.collect(value.toString());
			}
			out.collect(END_OF_GROUP);
		}
	}

	public static final class MySlidingBatchReduce2 extends
			RichGroupReduceFunction<Tuple2<Integer, String>, String> {
		private static final long serialVersionUID = 1L;

		String openString;

		@Override
		public void reduce(Iterable<Tuple2<Integer, String>> values, Collector<String> out)
				throws Exception {
			out.collect(openString);
			for (Tuple2<Integer, String> value : values) {
				out.collect(value.f0.toString());
			}
			out.collect(END_OF_GROUP);
		}

		@Override
		public void open(Configuration c) {
			openString = "open";
		}
	}

	private final static String END_OF_GROUP = "end of group";

	@SuppressWarnings("unchecked")
	@Test
	public void slidingBatchGroupReduceTest() {
		GroupedBatchGroupReduceInvokable<Integer, String> invokable1 = new GroupedBatchGroupReduceInvokable<Integer, String>(
				new MySlidingBatchReduce1(), 2, 2, 0);

		List<String> expected = Arrays.asList("1", "1", END_OF_GROUP, "3", "3", END_OF_GROUP, "2",
				END_OF_GROUP);
		List<String> actual = MockInvokable.createAndExecute(invokable1,
				Arrays.asList(1, 1, 2, 3, 3));

		assertEquals(expected, actual);

		GroupedBatchGroupReduceInvokable<Tuple2<Integer, String>, String> invokable2 = new GroupedBatchGroupReduceInvokable<Tuple2<Integer, String>, String>(
				new MySlidingBatchReduce2(), 2, 2, 1);

		expected = Arrays.asList("open", "1", "2", END_OF_GROUP, "open", "3", "3", END_OF_GROUP,
				"open", "4", END_OF_GROUP);
		actual = MockInvokable.createAndExecute(invokable2, Arrays.asList(
				new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(2, "a"),
				new Tuple2<Integer, String>(3, "b"), new Tuple2<Integer, String>(3, "b"),
				new Tuple2<Integer, String>(4, "a")));

		assertEquals(expected, actual);

	}

}
