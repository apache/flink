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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for connected streams.
 */
public class SelfConnectionITCase extends AbstractTestBase {

	/**
	 * We connect two different data streams in a chain to a CoMap.
	 */
	@Test
	public void differentDataStreamSameChain() throws Exception {

		TestListResultSink<String> resultSink = new TestListResultSink<String>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Integer> src = env.fromElements(1, 3, 5);

		DataStream<String> stringMap = src.map(new MapFunction<Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(Integer value) throws Exception {
				return "x " + value;
			}
		});

		stringMap.connect(src).map(new CoMapFunction<String, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map1(String value) {
				return value;
			}

			@Override
			public String map2(Integer value) {
				return String.valueOf(value + 1);
			}
		}).addSink(resultSink);

		env.execute();

		List<String> expected = Arrays.asList("x 1", "x 3", "x 5", "2", "4", "6");

		List<String> result = resultSink.getResult();

		Collections.sort(expected);
		Collections.sort(result);

		assertEquals(expected, result);
	}

	/**
	 * We connect two different data streams in different chains to a CoMap.
	 * (This is not actually self-connect.)
	 */
	@Test
	public void differentDataStreamDifferentChain() {

		TestListResultSink<String> resultSink = new TestListResultSink<String>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);

		DataStream<Integer> src = env.fromElements(1, 3, 5).disableChaining();

		DataStream<String> stringMap = src.flatMap(new FlatMapFunction<Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Integer value, Collector<String> out) throws Exception {
				out.collect("x " + value);
			}
		}).keyBy(new KeySelector<String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(String value) throws Exception {
				return value.length();
			}
		});

		DataStream<Long> longMap = src.map(new MapFunction<Integer, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long map(Integer value) throws Exception {
				return (long) (value + 1);
			}
		}).keyBy(new KeySelector<Long, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Long value) throws Exception {
				return value.intValue();
			}
		});

		stringMap.connect(longMap).map(new CoMapFunction<String, Long, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map1(String value) {
				return value;
			}

			@Override
			public String map2(Long value) {
				return value.toString();
			}
		}).addSink(resultSink);

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		List<String> expected = Arrays.asList("x 1", "x 3", "x 5", "2", "4", "6");
		List<String> result = resultSink.getResult();

		Collections.sort(expected);
		Collections.sort(result);

		assertEquals(expected, result);
	}
}
