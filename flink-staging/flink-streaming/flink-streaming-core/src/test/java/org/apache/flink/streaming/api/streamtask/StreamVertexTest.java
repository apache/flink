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

package org.apache.flink.streaming.api.streamtask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.junit.Test;

public class StreamVertexTest {

	private static Map<Integer, Integer> data = new HashMap<Integer, Integer>();

	public static class MySource implements SourceFunction<Tuple1<Integer>> {
		private Tuple1<Integer> tuple = new Tuple1<Integer>(0);

		private int i = 0;

		@Override
		public boolean reachedEnd() throws Exception {
			return i >= 10;
		}

		@Override
		public Tuple1<Integer> next() throws Exception {
			tuple.f0 = i;
			i++;
			return tuple;
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

	@Test
	public void wrongJobGraph() {
		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(SOURCE_PARALELISM);

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
			env.generateSequence(1, 10).project(2);
			fail();
		} catch (RuntimeException e) {
		}
	}

	private static class CoMap implements CoMapFunction<String, Long, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) {
			// System.out.println(value);
			return value;
		}

		@Override
		public String map2(Long value) {
			// System.out.println(value);
			return value.toString();
		}
	}

	private static class SetSink implements SinkFunction<String> {
		private static final long serialVersionUID = 1L;
		public static Set<String> result = Collections.synchronizedSet(new HashSet<String>());

		@Override
		public void invoke(String value) {
			result.add(value);
		}

	}

	@Test
	public void coTest() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(SOURCE_PARALELISM, MEMORYSIZE);

		DataStream<String> fromStringElements = env.fromElements("aa", "bb", "cc");
		DataStream<Long> generatedSequence = env.generateParallelSequence(0, 3);

		fromStringElements.connect(generatedSequence).map(new CoMap()).addSink(new SetSink());

		env.execute();

		HashSet<String> expectedSet = new HashSet<String>(Arrays.asList("aa", "bb", "cc", "0", "1",
				"2", "3"));
		assertEquals(expectedSet, SetSink.result);
	}

	@Test
	public void runStream() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(SOURCE_PARALELISM, MEMORYSIZE);

		env.addSource(new MySource()).setParallelism(SOURCE_PARALELISM).map(new MyTask())
				.addSink(new MySink());

		env.execute();
		assertEquals(10, data.keySet().size());

		for (Integer k : data.keySet()) {
			assertEquals((Integer) (k + 1), data.get(k));
		}
	}
}
