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

package org.apache.flink.streaming.api.operators;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.MockContext;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlatMapTest {

	public static final class MyFlatMap implements FlatMapFunction<Integer, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			if (value % 2 == 0) {
				out.collect(value);
				out.collect(value * value);
			}
		}
	}

	@Test
	public void flatMapTest() {
		StreamFlatMap<Integer, Integer> operator = new StreamFlatMap<Integer, Integer>(new MyFlatMap());
		
		List<Integer> expected = Arrays.asList(2, 4, 4, 16, 6, 36, 8, 64);
		List<Integer> actual = MockContext.createAndExecute(operator, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
		
		assertEquals(expected, actual);
	}
}
