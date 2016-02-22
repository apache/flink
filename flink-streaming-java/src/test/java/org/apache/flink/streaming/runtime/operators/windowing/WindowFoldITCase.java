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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Folds over windows. These also test whether OutputTypeConfigurable functions
 * work for windows, because FoldWindowFunction is OutputTypeConfigurable.
 */
@SuppressWarnings("serial")
public class WindowFoldITCase extends StreamingMultipleProgramsTestBase {

	private static List<String> testResults;

	@Test
	public void testFoldWindow() throws Exception {

		testResults = new ArrayList<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> source1 = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				ctx.collect(Tuple2.of("a", 0));
				ctx.collect(Tuple2.of("a", 1));
				ctx.collect(Tuple2.of("a", 2));

				ctx.collect(Tuple2.of("b", 3));
				ctx.collect(Tuple2.of("b", 4));
				ctx.collect(Tuple2.of("b", 5));

				ctx.collect(Tuple2.of("a", 6));
				ctx.collect(Tuple2.of("a", 7));
				ctx.collect(Tuple2.of("a", 8));

				// so that we get a high final watermark to process the previously sent elements
				ctx.collect(Tuple2.of("a", 20));
			}

			@Override
			public void cancel() {
			}
		}).assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());

		source1
				.keyBy(0)
				.window(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
				.fold(Tuple2.of("R:", 0), new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator,
							Tuple2<String, Integer> value) throws Exception {
						accumulator.f0 += value.f0;
						accumulator.f1 += value.f1;
						return accumulator;
					}
				})
				.addSink(new SinkFunction<Tuple2<String, Integer>>() {
					@Override
					public void invoke(Tuple2<String, Integer> value) throws Exception {
						testResults.add(value.toString());
					}
				});

		env.execute("Fold Window Test");

		List<String> expectedResult = Arrays.asList(
				"(R:aaa,3)",
				"(R:aaa,21)",
				"(R:bbb,12)");

		Collections.sort(expectedResult);
		Collections.sort(testResults);

		Assert.assertEquals(expectedResult, testResults);
	}

	@Test
	public void testFoldAllWindow() throws Exception {

		testResults = new ArrayList<>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		DataStream<Tuple2<String, Integer>> source1 = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {

			@Override
			public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
				ctx.collect(Tuple2.of("a", 0));
				ctx.collect(Tuple2.of("a", 1));
				ctx.collect(Tuple2.of("a", 2));

				ctx.collect(Tuple2.of("b", 3));
				ctx.collect(Tuple2.of("a", 3));
				ctx.collect(Tuple2.of("b", 4));
				ctx.collect(Tuple2.of("a", 4));
				ctx.collect(Tuple2.of("b", 5));
				ctx.collect(Tuple2.of("a", 5));

				// so that we get a high final watermark to process the previously sent elements
				ctx.collect(Tuple2.of("a", 20));
			}

			@Override
			public void cancel() {
			}
		}).assignTimestampsAndWatermarks(new Tuple2TimestampExtractor());

		source1
				.windowAll(TumblingTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
				.fold(Tuple2.of("R:", 0), new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator,
							Tuple2<String, Integer> value) throws Exception {
						accumulator.f0 += value.f0;
						accumulator.f1 += value.f1;
						return accumulator;
					}
				})
				.addSink(new SinkFunction<Tuple2<String, Integer>>() {
					@Override
					public void invoke(Tuple2<String, Integer> value) throws Exception {
						testResults.add(value.toString());
					}
				});

		env.execute("Fold All-Window Test");

		List<String> expectedResult = Arrays.asList(
				"(R:aaa,3)",
				"(R:bababa,24)");

		Collections.sort(expectedResult);
		Collections.sort(testResults);

		Assert.assertEquals(expectedResult, testResults);
	}

	private static class Tuple2TimestampExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<String, Integer>> {

		@Override
		public long extractTimestamp(Tuple2<String, Integer> element, long previousTimestamp) {
			return element.f1;
		}

		@Override
		public Watermark checkAndGetNextWatermark(Tuple2<String, Integer> lastElement, long extractedTimestamp) {
			return new Watermark(lastElement.f1 - 1);
		}
	}
}
