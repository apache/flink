/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.LegacyKeyedProcessOperator;
import org.apache.flink.streaming.api.testutils.StreamTestUtils;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link KeyedStream}.
 */
public class KeyedStreamTest {

	/**
	 * Verify that a {@link KeyedStream#process(ProcessFunction)} call is correctly translated to an operator.
	 */
	@Test
	@Deprecated
	public void testKeyedStreamProcessTranslation() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Long> src = env.generateSequence(0, 0);

		ProcessFunction<Long, Integer> processFunction = new ProcessFunction<Long, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(
					Long value,
					Context ctx,
					Collector<Integer> out) throws Exception {
				// Do nothing
			}

			@Override
			public void onTimer(
					long timestamp,
					OnTimerContext ctx,
					Collector<Integer> out) throws Exception {
				// Do nothing
			}
		};

		DataStream<Integer> processed = src
				.keyBy(new DataStreamTest.IdentityKeySelector<Long>())
				.process(processFunction);

		processed.addSink(new DiscardingSink<Integer>());

		assertEquals(processFunction, StreamTestUtils.getFunctionForDataStream(processed));
		assertTrue(StreamTestUtils.getOperatorForDataStream(processed) instanceof LegacyKeyedProcessOperator);
	}

	/**
	 * Verify that a {@link KeyedStream#process(KeyedProcessFunction)} call is correctly translated to an operator.
	 */
	@Test
	public void testKeyedStreamKeyedProcessTranslation() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Long> src = env.generateSequence(0, 0);

		KeyedProcessFunction<Long, Long, Integer> keyedProcessFunction = new KeyedProcessFunction<Long, Long, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void processElement(Long value, Context ctx, Collector<Integer> out) throws Exception {
				// Do nothing
			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
				// Do nothing
			}
		};

		DataStream<Integer> processed = src
				.keyBy(new DataStreamTest.IdentityKeySelector<Long>())
				.process(keyedProcessFunction);

		processed.addSink(new DiscardingSink<Integer>());

		assertEquals(keyedProcessFunction, StreamTestUtils.getFunctionForDataStream(processed));
		assertTrue(StreamTestUtils.getOperatorForDataStream(processed) instanceof KeyedProcessOperator);
	}
}
