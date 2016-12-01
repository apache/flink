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
import org.apache.flink.api.common.typeinfo.OutputTag;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.outputtags.LateArrivingOutputTag;
import org.apache.flink.util.Collector;
import org.apache.flink.util.CollectorWrapper;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration test for streaming programs using sideOutputs
 *
 * See FLINK-4460
 */
public class SideOutputITCase extends StreamingMultipleProgramsTestBase{

	static List<Integer> elements = new ArrayList<>();
	static {
		elements.add(1);
		elements.add(2);
		elements.add(5);
		elements.add(3);
		elements.add(4);
	}

	/**
	 * Serializable outputTag used as sideOutput
	 */
	static class SideOutputTag extends OutputTag<String>{
		public SideOutputTag(String value){
			super(value);
		}
	}

	/**
	 * Test flatMap sideOutputs
	 */
	@Test
	public void testFlatMapSideOutputs() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.disableOperatorChaining();
		see.setParallelism(3);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		SingleOutputStreamOperator<Integer> passThroughtStream = dataStream.flatMap(new FlatMapFunction<Integer, Integer>() {

			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value);
				CollectorWrapper<Integer> wrapper = new CollectorWrapper<>(out);
				wrapper.collect(new SideOutputTag("side"), "sideout-" + String.valueOf(value));
			}
		});

		passThroughtStream.getSideOutput(new SideOutputTag("side")).print();
		passThroughtStream.print();

		passThroughtStream.getSideOutput(new SideOutputTag("notside")).print();
		see.execute();
	}

	/**
	 * Test flatMap sideOutputs with different outputTag
	 */
	@Test
	public void testFlatMapSideOutputsWithWrongTag() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.disableOperatorChaining();
		see.setParallelism(3);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		dataStream.flatMap(new FlatMapFunction<Integer, Integer>() {

			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value);
				CollectorWrapper<Integer> wrapper = new CollectorWrapper<>(out);
				wrapper.collect(new SideOutputTag("side"), "sideout-" + String.valueOf(value));
			}
		}).getSideOutput(new SideOutputTag("notside")).print();
		see.execute();
	}

	private static class TestWatermarkAssigner implements AssignerWithPunctuatedWatermarks<Integer>{
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

	private static class TestKeySelector implements KeySelector<Integer, Integer>{
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
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.disableOperatorChaining();
		see.setParallelism(3);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		SingleOutputStreamOperator<Integer> outputStreamOperator = dataStream.assignTimestampsAndWatermarks(
			new TestWatermarkAssigner()).timeWindowAll(Time.milliseconds(1), Time.milliseconds(1))
				.apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
					@Override
					public void apply(TimeWindow window, Iterable<Integer> values, Collector<Integer> out) throws Exception {
							for(Integer val : values) {
								out.collect(val);
							}
					}
				});

		outputStreamOperator.getSideOutput(new LateArrivingOutputTag<Integer>())
			.flatMap(new FlatMapFunction<StreamRecord<Integer>, String>() {
				@Override
				public void flatMap(StreamRecord<Integer> value, Collector<String> out) throws Exception {
					out.collect("late-" + String.valueOf(value.getValue()) + "-ts" + String.valueOf(value.getTimestamp()));
				}
			}).print();
		see.execute();
	}

	@Test
	public void testKeyedWindowLateArrivingEvents() throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.disableOperatorChaining();
		see.setParallelism(3);
		see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Integer> dataStream = see.fromCollection(elements);

		SingleOutputStreamOperator<String> outputStreamOperator = dataStream.assignTimestampsAndWatermarks(
			new TestWatermarkAssigner()).keyBy(new TestKeySelector()).timeWindow(Time.milliseconds(1), Time.milliseconds(1))
				.apply(new WindowFunction<Integer, String, Integer, TimeWindow>() {
			@Override
			public void apply(Integer key, TimeWindow window, Iterable<Integer> input, Collector<String> out) throws Exception {
				CollectorWrapper<String> sideOuput = new CollectorWrapper<String>(out);
				for(Integer val : input) {
					out.collect(String.valueOf(key) + "-"+String.valueOf(val));
					sideOuput.collect(new SideOutputTag("applySideOutput"), "apply-" + String.valueOf(val));
				}
			}
		});

		outputStreamOperator.getSideOutput(new SideOutputTag("applySideOutput")).print();

		outputStreamOperator.getSideOutput(new LateArrivingOutputTag<Integer>())
			.flatMap(new FlatMapFunction<StreamRecord<Integer>, String>() {
				@Override
				public void flatMap(StreamRecord<Integer> value, Collector<String> out) throws Exception {
					out.collect("late-" + String.valueOf(value.getValue()) + "-ts" + String.valueOf(value.getTimestamp()));
				}
		}).print();
		see.execute();
	}

}
