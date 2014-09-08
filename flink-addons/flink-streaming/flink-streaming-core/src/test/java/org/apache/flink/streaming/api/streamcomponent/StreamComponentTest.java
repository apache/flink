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

package org.apache.flink.streaming.api.streamcomponent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.util.ClusterUtil;
import org.apache.flink.util.Collector;
import org.junit.Ignore;
import org.junit.Test;

public class StreamComponentTest {

	private static Map<Integer, Integer> data = new HashMap<Integer, Integer>();

	public static class MySource implements SourceFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		private Tuple1<Integer> tuple = new Tuple1<Integer>(0);

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 10; i++) {
				tuple.f0 = i;
				collector.collect(tuple);
			}
		}
	}

	public static class MyTask extends RichMapFunction<Tuple1<Integer>, Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(Tuple1<Integer> value) throws Exception {
			Integer i = value.f0;
			return new Tuple2<Integer, Integer>(i, i + 1);
		}
	}

	public static class MySink implements SinkFunction<Tuple2<Integer, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple2<Integer, Integer> tuple) {
			Integer k = tuple.getField(0);
			Integer v = tuple.getField(1);
			data.put(k, v);
		}
	}

	@SuppressWarnings("unused")
	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALELISM = 1;
	private static final long MEMORYSIZE = 32;

	@Ignore
	@Test
	public void wrongJobGraph() {
		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(SOURCE_PARALELISM);

		try {
			env.execute();
			fail();
		} catch (RuntimeException e) {
			assertEquals(e.getMessage(), ClusterUtil.CANNOT_EXECUTE_EMPTY_JOB);
		}

		env.fromCollection(Arrays.asList("a", "b"));

		try {
			env.execute();
			fail();
		} catch (RuntimeException e) {
			System.out.println(e.getMessage());
		}

		try {
			env.fromCollection(null);
			fail();
		} catch (NullPointerException e) {
		}

		try {
			env.fromElements();
			fail();
		} catch (IllegalArgumentException e) {
		}

		try {
			env.generateSequence(-10, -30);
			fail();
		} catch (IllegalArgumentException e) {
		}

		try {
			env.setBufferTimeout(-10);
			fail();
		} catch (IllegalArgumentException e) {
		}

		try {
			env.setExecutionParallelism(-10);
			fail();
		} catch (IllegalArgumentException e) {
		}

		try {
			env.readTextFile("random/path/that/is/not/valid");
			fail();
		} catch (IllegalArgumentException e) {
		}
	}

	private static class CoMap implements CoMapFunction<String, Long, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) {
			return value;
		}

		@Override
		public String map2(Long value) {
			return value.toString();
		}
	}

	static HashSet<String> resultSet;
	private static class SetSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(String value) {
			resultSet.add(value);
		}
	}
	
	@Test
	public void coTest() {
		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(SOURCE_PARALELISM);

		DataStream<String> fromStringElements = env.fromElements("aa", "bb", "cc");
		DataStream<Long> generatedSequence = env.generateSequence(0, 3);
		
		fromStringElements.connect(generatedSequence).map(new CoMap()).addSink(new SetSink());
		
		resultSet = new HashSet<String>();
		env.execute();
		
		HashSet<String> expectedSet = new HashSet<String>(Arrays.asList("aa", "bb", "cc", "0", "1", "2", "3"));
		assertEquals(expectedSet, resultSet);
	}

	@Test
	public void runStream() {
		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(SOURCE_PARALELISM);

		env.addSource(new MySource(), SOURCE_PARALELISM).map(new MyTask()).addSink(new MySink());

		env.executeTest(MEMORYSIZE);

		assertEquals(10, data.keySet().size());

		for (Integer k : data.keySet()) {
			assertEquals((Integer) (k + 1), data.get(k));
		}
	}
}
