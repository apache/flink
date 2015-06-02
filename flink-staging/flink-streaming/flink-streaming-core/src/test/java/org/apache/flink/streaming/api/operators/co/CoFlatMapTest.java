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

package org.apache.flink.streaming.api.operators.co;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.util.MockCoContext;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class CoFlatMapTest implements Serializable {
	private static final long serialVersionUID = 1L;

	private final static class MyCoFlatMap implements CoFlatMapFunction<String, Integer, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap1(String value, Collector<String> coll) {
			for (int i = 0; i < value.length(); i++) {
				coll.collect(value.substring(i, i + 1));
			}
		}

		@Override
		public void flatMap2(Integer value, Collector<String> coll) {
			coll.collect(value.toString());
		}
	}

	@Test
	public void coFlatMapTest() {
		CoStreamFlatMap<String, Integer, String> invokable = new CoStreamFlatMap<String, Integer, String>(
				new MyCoFlatMap());

		List<String> expectedList = Arrays.asList("a", "b", "c", "1", "d", "e", "f", "2", "g", "h",
				"e", "3", "4", "5");
		List<String> actualList = MockCoContext.createAndExecute(invokable,
				Arrays.asList("abc", "def", "ghe"), Arrays.asList(1, 2, 3, 4, 5));

		assertEquals(expectedList, actualList);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void multipleInputTest() {
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		DataStream<Integer> ds1 = env.fromElements(1, 3, 5);
		DataStream<Integer> ds2 = env.fromElements(2, 4).union(ds1);
		
		try {
			ds1.forward().union(ds2);
			fail();
		} catch (RuntimeException e) {
			// expected
		}
		
	}
}
