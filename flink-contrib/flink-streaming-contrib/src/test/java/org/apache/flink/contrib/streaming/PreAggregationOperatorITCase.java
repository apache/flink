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

package org.apache.flink.contrib.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PreAggregationOperator}.
 */
public class PreAggregationOperatorITCase extends TestLogger {

	private static LocalFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		cluster = new LocalFlinkMiniCluster(new Configuration(), false);
		cluster.start();
		TestStreamEnvironment.setAsContext(cluster, 1);
	}

	@AfterClass
	public static void shutdownCluster() {
		cluster.stop();
		TestStreamEnvironment.unsetAsContext();
	}

	@Test
	public void testPreAggregate() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Long, Long>> stream = env.generateSequence(0, (long) 19)
			.assignTimestampsAndWatermarks(new WatermarksAssigner())
			.map(new GenerateInput());

		SlidingEventTimeWindows windows = SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(10));
		SingleOutputStreamOperator<Long> result = DataStreamUtils.aggregateWithPreAggregation(
			stream,
			new FirstTupleFieldKeySelector(),
			new SumSecondTupleFieldAggregation(),
			windows,
			false);

		List<Long> actualResult = new ArrayList<>();
		DataStreamUtils.collect(result).forEachRemaining(actualResult::add);

		assertEquals(Arrays.asList(5L, 5L, 5L, 5L), actualResult);
	}

	private static class FirstTupleFieldKeySelector implements KeySelector<Tuple2<Long, Long>, Long> {
		@Override
		public Long getKey(Tuple2<Long, Long> value) throws Exception {
			return value.f0;
		}
	}

	private static class GenerateInput implements MapFunction<Long, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Long value) throws Exception {
			return Tuple2.of(value % 2, 1L);
		}
	}

	private static class SumSecondTupleFieldAggregation implements AggregateFunction<Tuple2<Long, Long>, Long, Long> {
		@Override
		public Long createAccumulator() {
			return new Long(0);
		}

		@Override
		public Long add(Tuple2<Long, Long> value, Long accumulator) {
			return accumulator + value.f1;
		}

		@Override
		public Long getResult(Long accumulator) {
			return accumulator;
		}

		@Override
		public Long merge(Long a, Long b) {
			return a + b;
		}
	}

	private static class WatermarksAssigner implements AssignerWithPunctuatedWatermarks<Long> {
		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
			return new Watermark(extractedTimestamp);
		}

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			return element;
		}
	}
}
