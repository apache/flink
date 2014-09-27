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
import java.util.Collections;
import java.util.List;

import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoBatchGroupReduceInvokable;
import org.apache.flink.streaming.util.MockCoInvokable;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoBatchGroupReduceTest {

	public static final class MyCoGroupReduceFunction implements
			CoGroupReduceFunction<Integer, String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce1(Iterable<Integer> values, Collector<String> out) throws Exception {
			String gather = "";
			for (Integer value : values) {
				gather += value.toString();
			}
			out.collect(gather);
		}

		@Override
		public void reduce2(Iterable<String> values, Collector<String> out) throws Exception {
			String gather = "";
			for (String value : values) {
				gather += value;
			}
			out.collect(gather);
		}
	}

	@Test
	public void coBatchGroupReduceTest1() {

		List<Integer> inputs1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		List<String> inputs2 = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");

		CoBatchGroupReduceInvokable<Integer, String, String> invokable = new CoBatchGroupReduceInvokable<Integer, String, String>(
				new MyCoGroupReduceFunction(), 4L, 2L, 4L, 2L);

		List<String> expected = Arrays.asList("1234", "5678", "ab", "cd", "ef", "gh");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

	@Test
	public void coBatchGroupReduceTest2() {

		List<Integer> inputs1 = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		List<String> inputs2 = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");

		CoBatchGroupReduceInvokable<Integer, String, String> invokable = new CoBatchGroupReduceInvokable<Integer, String, String>(
				new MyCoGroupReduceFunction(), 4L, 2L, 3L, 1L);

		List<String> expected = Arrays.asList("1234", "4567", "78910", "ab", "bc", "cd", "de",
				"ef", "fg", "gh");

		List<String> actualList = MockCoInvokable.createAndExecute(invokable, inputs1, inputs2);
		Collections.sort(expected);
		Collections.sort(actualList);

		assertEquals(expected, actualList);
	}

}
