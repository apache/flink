/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils.AccumulatorCountingSink;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests latency marker.
 */
public class LatencyMarkerITCase {
	/**
	 * FLINK-17780: Tests that streams are not corrupted/records lost when using latency markers with broadcast.
	 */
	@Test
	public void testBroadcast() throws Exception {
		int inputCount = 100000;
		int parallelism = 4;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setLatencyTrackingInterval(2000);
		env.setRestartStrategy(RestartStrategies.noRestart());

		List<Integer> broadcastData = IntStream.range(0, inputCount).boxed().collect(Collectors.toList());
		DataStream<Integer> broadcastDataStream = env.fromCollection(broadcastData)
			.setParallelism(1);

		// broadcast the configurations and create the broadcast state

		DataStream<String> streamWithoutData = env.fromCollection(Collections.emptyList(), TypeInformation.of(String.class));

		MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("BroadcastState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		SingleOutputStreamOperator<Integer> processor = streamWithoutData
			.connect(broadcastDataStream.broadcast(stateDescriptor))
			.process(new BroadcastProcessFunction<String, Integer, Integer>() {
				int expected = 0;

				public void processElement(String value, ReadOnlyContext ctx, Collector<Integer> out) {
				}

				public void processBroadcastElement(Integer value, Context ctx, Collector<Integer> out) {
					if (value != expected++) {
						throw new AssertionError(String.format("Value was supposed to be: '%s', but was: '%s'", expected - 1, value));
					}
					out.collect(value);
				}
			});

		processor.addSink(new AccumulatorCountingSink<>())
			.setParallelism(1);

		JobExecutionResult executionResult = env.execute();

		Integer count = executionResult.getAccumulatorResult(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR);
		Assert.assertEquals(inputCount * parallelism, count.intValue());
	}
}
