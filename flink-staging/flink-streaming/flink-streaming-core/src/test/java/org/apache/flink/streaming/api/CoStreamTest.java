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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.util.TestListResultSink;
import org.apache.flink.streaming.util.TestStreamEnvironment;

public class CoStreamTest {

	private static final long MEMORY_SIZE = 32;

	private static ArrayList<String> expected;

	public static void main(String[] args) throws InterruptedException {
		for (int i = 0; i < 200; i++) {
			test();
		}
	}

	//	@Test
	public static void test() {
		expected = new ArrayList<String>();

		StreamExecutionEnvironment env = new TestStreamEnvironment(3, MEMORY_SIZE);

		TestListResultSink<String> resultSink = new TestListResultSink<String>();

		DataStream<Integer> src = env.fromElements(1, 3, 5);
		DataStream<Integer> src2 = env.fromElements(1, 3, 5);

		DataStream<Integer> grouped = src.groupBy(new KeySelector<Integer, Integer>() {
			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		});

		DataStream<Integer> grouped2 = src2.groupBy(new KeySelector<Integer, Integer>() {
			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		});

		DataStream<String> connected = grouped.connect(grouped2).map(new CoMapFunction<Integer, Integer, String>() {
			@Override
			public String map1(Integer value) {
				return value.toString();
			}

			@Override
			public String map2(Integer value) {
				return value.toString();
			}
		});

		connected.addSink(resultSink);

		connected.print();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		expected = new ArrayList<String>();
		expected.addAll(Arrays.asList("1", "1", "3", "3", "5", "5"));

		System.out.println(resultSink.getResult());
		assertEquals(expected, expected);
	}
}