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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataStreamTest {

	private static final long MEMORYSIZE = 32;
	private static int PARALLELISM = 1;

	@Test
	public void testNaming() throws Exception {
		StreamExecutionEnvironment env = new TestStreamEnvironment(PARALLELISM, MEMORYSIZE);

		DataStream<Long> dataStream1 = env.generateSequence(0, 0).name("testSource1")
				.map(new MapFunction<Long, Long>() {
					@Override
					public Long map(Long value) throws Exception {
						return null;
					}
				}).name("testMap");

		DataStream<Long> dataStream2 = env.generateSequence(0, 0).name("testSource2")
				.reduce(new ReduceFunction<Long>() {
					@Override
					public Long reduce(Long value1, Long value2) throws Exception {
						return null;
					}
				}).name("testReduce");

		DataStream<Long> connected = dataStream1.connect(dataStream2)
				.flatMap(new CoFlatMapFunction<Long, Long, Long>() {
					@Override
					public void flatMap1(Long value, Collector<Long> out) throws Exception {
					}

					@Override
					public void flatMap2(Long value, Collector<Long> out) throws Exception {
					}
				}).name("testCoFlatMap")
				.window(Count.of(10))
				.foldWindow(0L, new FoldFunction<Long, Long>() {
					@Override
					public Long fold(Long accumulator, Long value) throws Exception {
						return null;
					}
				}).name("testWindowFold")
				.flatten();

		//test functionality through the operator names in the execution plan
		String plan = env.getExecutionPlan();

		assertTrue(plan.contains("testSource1"));
		assertTrue(plan.contains("testSource2"));
		assertTrue(plan.contains("testMap"));
		assertTrue(plan.contains("testReduce"));
		assertTrue(plan.contains("testCoFlatMap"));
		assertTrue(plan.contains("testWindowFold"));
	}

}
