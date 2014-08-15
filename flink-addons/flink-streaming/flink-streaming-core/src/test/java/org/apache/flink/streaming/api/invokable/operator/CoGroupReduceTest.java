/** Licensed to the Apache Software Foundation (ASF) under one or more
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
 *
 */

package org.apache.flink.streaming.api.invokable.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class CoGroupReduceTest {

	private static List<String> result;
	private static List<String> expected = new ArrayList<String>();

	private final static class EmptySink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(String tuple) {
		}
	}

	private final static class MyCoReduceFunction implements
			CoReduceFunction<Tuple3<String, String, String>, Tuple2<Integer, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, String, String> reduce1(Tuple3<String, String, String> value1,
				Tuple3<String, String, String> value2) {
			return new Tuple3<String, String, String>(value1.f0, value1.f1 + value2.f1, value1.f2);
		}

		@Override
		public Tuple2<Integer, Integer> reduce2(Tuple2<Integer, Integer> value1,
				Tuple2<Integer, Integer> value2) {
			return new Tuple2<Integer, Integer>(value1.f0, value1.f1 + value2.f1);
		}

		@Override
		public String map1(Tuple3<String, String, String> value) {
			String mapResult = value.f1;
			result.add(mapResult);
			return mapResult;
		}

		@Override
		public String map2(Tuple2<Integer, Integer> value) {
			String mapResult = value.f1.toString();
			result.add(mapResult);
			return mapResult;
		}
	}

	@Test
	public void multipleInputTest() {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);
		expected.add("word1word3");
		expected.add("word2");
		expected.add("3");
		expected.add("5");
		expected.add("7");
		Tuple3<String, String, String> word1 = new Tuple3<String, String, String>("a", "word1", "b");
		Tuple3<String, String, String> word2 = new Tuple3<String, String, String>("b", "word2", "a");
		Tuple3<String, String, String> word3 = new Tuple3<String, String, String>("a", "word3", "a");
		Tuple2<Integer, Integer> int1 = new Tuple2<Integer, Integer>(2, 1);
		Tuple2<Integer, Integer> int2 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> int3 = new Tuple2<Integer, Integer>(0, 3);
		Tuple2<Integer, Integer> int4 = new Tuple2<Integer, Integer>(2, 4);
		Tuple2<Integer, Integer> int5 = new Tuple2<Integer, Integer>(1, 5);

		result = new ArrayList<String>();

		LocalStreamEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unchecked")
		DataStream<Tuple2<Integer, Integer>> ds1 = env1.fromElements(int1, int3, int5);
		@SuppressWarnings("unchecked")
		DataStream<Tuple2<Integer, Integer>> ds2 = env1.fromElements(int2, int4).merge(ds1);

		@SuppressWarnings({ "unused", "unchecked" })
		DataStream<String> ds4 = env1.fromElements(word1, word2, word3).connect(ds2).groupBy(0, 0)
				.reduce(new MyCoReduceFunction(), 0, 0).addSink(new EmptySink());

		env1.executeTest(32);

		Assert.assertEquals(result.size(), 8);
		Assert.assertTrue(result.containsAll(expected));
	}

	@Test
	public void multipleInputTest2() {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);
		expected.clear();
		result.clear();
		expected.add("word2word3");
		expected.add("word1");
		expected.add("3");
		expected.add("5");
		expected.add("7");
		Tuple3<String, String, String> word1 = new Tuple3<String, String, String>("a", "word1", "b");
		Tuple3<String, String, String> word2 = new Tuple3<String, String, String>("b", "word2", "a");
		Tuple3<String, String, String> word3 = new Tuple3<String, String, String>("a", "word3", "a");
		Tuple2<Integer, Integer> int1 = new Tuple2<Integer, Integer>(2, 1);
		Tuple2<Integer, Integer> int2 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> int3 = new Tuple2<Integer, Integer>(0, 3);
		Tuple2<Integer, Integer> int4 = new Tuple2<Integer, Integer>(2, 4);
		Tuple2<Integer, Integer> int5 = new Tuple2<Integer, Integer>(1, 5);

		result = new ArrayList<String>();

		LocalStreamEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unchecked")
		DataStream<Tuple2<Integer, Integer>> ds1 = env2.fromElements(int1, int3, int5);
		@SuppressWarnings("unchecked")
		DataStream<Tuple2<Integer, Integer>> ds2 = env2.fromElements(int2, int4).merge(ds1);

		@SuppressWarnings({ "unused", "unchecked" })
		DataStream<String> ds4 = env2.fromElements(word1, word2, word3).connect(ds2).groupBy(2, 0)
				.reduce(new MyCoReduceFunction(), 2, 0).addSink(new EmptySink());

		env2.executeTest(32);

		Assert.assertEquals(result.size(), 8);
		Assert.assertTrue(result.containsAll(expected));
	}
}
