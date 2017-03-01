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
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.util.Collector;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration test for streaming programs using side outputs.
 */
public class SideOutputITCase extends StreamingMultipleProgramsTestBase {

	static List<Integer> elements = new ArrayList<>();
	static {
		elements.add(1);
		elements.add(2);
		elements.add(5);
		elements.add(3);
		elements.add(4);
	}

	private static class TestWatermarkAssigner implements AssignerWithPunctuatedWatermarks<Integer> {
		private static final long serialVersionUID = 1L;

		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(Integer lastElement, long extractedTimestamp) {
			return new Watermark(extractedTimestamp);
		}

		@Override
		public long extractTimestamp(Integer element, long previousElementTimestamp) {
			return Long.valueOf(element);
		}
	}

	private static class TestKeySelector implements KeySelector<Integer, Integer> {
		private static final long serialVersionUID = 1L;
		
		@Override
		public Integer getKey(Integer value) throws Exception {
			return value;
		}
	}

	/**
	 * Test window late arriving events stream
	 */
	@Test
	public void testAllWindowLateArrivingEvents() throws Exception {
		TestListResultSink<String> sideOutputResultSink = new TestListResultSink<>();

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		OutputTag<Integer> lateDataTag = new OutputTag<Integer>("late"){};

		SingleOutputStreamOperator<Integer> windowOperator = dataStream
				.assignTimestampsAndWatermarks(new TestWatermarkAssigner())
				.timeWindowAll(Time.milliseconds(1), Time.milliseconds(1))
				.sideOutputLateData(lateDataTag)
				.apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
					private static final long serialVersionUID = 1L;
					
					@Override
					public void apply(TimeWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
							for(Integer val : values) {
								out.collect(val);
							}
					}
				});

		windowOperator
				.getSideOutput(lateDataTag)
				.flatMap(new FlatMapFunction<Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(Integer value, Collector<String> out) throws Exception {
						out.collect("late-" + String.valueOf(value));
					}
				})
				.addSink(sideOutputResultSink);

		see.execute();
		assertEquals(sideOutputResultSink.getSortedResult(), Arrays.asList("late-3", "late-4"));

	}

	@Test
	public void testKeyedWindowLateArrivingEvents() throws Exception {
		TestListResultSink<String> resultSink = new TestListResultSink<>();
		TestListResultSink<Integer> lateResultSink = new TestListResultSink<>();

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(3);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		OutputTag<Integer> lateDataTag = new OutputTag<Integer>("late"){};

		SingleOutputStreamOperator<String> windowOperator = dataStream
				.assignTimestampsAndWatermarks(new TestWatermarkAssigner())
				.keyBy(new TestKeySelector())
				.timeWindow(Time.milliseconds(1), Time.milliseconds(1))
				.allowedLateness(Time.milliseconds(2))
				.sideOutputLateData(lateDataTag)
				.apply(new WindowFunction<Integer, String, Integer, TimeWindow>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(Integer key, TimeWindow window, Iterable<Integer> input, Collector<String> out) throws Exception {
						for(Integer val : input) {
							out.collect(String.valueOf(key) + "-" + String.valueOf(val));
						}
					}
				});

		windowOperator
				.addSink(resultSink);

		windowOperator
				.getSideOutput(lateDataTag)
				.addSink(lateResultSink);

		see.execute();
		assertEquals(Arrays.asList("1-1", "2-2", "4-4", "5-5"), resultSink.getSortedResult());
		assertEquals(Collections.singletonList(3), lateResultSink.getSortedResult());
	}

}
