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

package org.apache.flink.cep;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;

@SuppressWarnings("serial")
public class CEPITCase extends StreamingMultipleProgramsTestBase {

	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
		expected = "";
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expected, resultPath);
	}

	/**
	 * Checks that a certain event sequence is recognized
	 * @throws Exception
	 */
	@Test
	public void testSimplePatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new Event(5, "middle", 5.0),
			new SubEvent(6, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(8, "end", 1.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
		.followedBy("middle").subtype(SubEvent.class).where(
				new FilterFunction<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
		.followedBy("end").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// expected sequence of matching event ids
		expected = "2,6,8";

		env.execute();
	}

	@Test
	public void testSimpleKeyedPatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Event> input = env.fromElements(
			new Event(1, "barfoo", 1.0),
			new Event(2, "start", 2.0),
			new Event(3, "start", 2.1),
			new Event(3, "foobar", 3.0),
			new SubEvent(4, "foo", 4.0, 1.0),
			new SubEvent(3, "middle", 3.2, 1.0),
			new Event(42, "start", 3.1),
			new SubEvent(42, "middle", 3.3, 1.2),
			new Event(5, "middle", 5.0),
			new SubEvent(2, "middle", 6.0, 2.0),
			new SubEvent(7, "bar", 3.0, 3.0),
			new Event(42, "42", 42.0),
			new Event(3, "end", 2.0),
			new Event(2, "end", 1.0),
			new Event(42, "end", 42.0)
		).keyBy(new KeySelector<Event, Integer>() {

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
			.followedBy("middle").subtype(SubEvent.class).where(
				new FilterFunction<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.followedBy("end").where(new FilterFunction<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, Event> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").getId()).append(",")
					.append(pattern.get("middle").getId()).append(",")
					.append(pattern.get("end").getId());

				return builder.toString();
			}
		});

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// the expected sequences of matching event ids
		expected = "2,2,2\n3,3,3\n42,42,42";

		env.execute();
	}

	@Test
	public void testSimplePatternEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "middle", 2.0), 1L),
			Tuple2.of(new Event(3, "end", 3.0), 3L),
			Tuple2.of(new Event(4, "end", 4.0), 10L),
			Tuple2.of(new Event(5, "middle", 5.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(5, "middle", 5.0), 100L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event,Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long previousTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, Event> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").getId()).append(",")
						.append(pattern.get("middle").getId()).append(",")
						.append(pattern.get("end").getId());

					return builder.toString();
				}
			}
		);

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// the expected sequence of matching event ids
		expected = "1,5,4";

		env.execute();
	}

	@Test
	public void testSimpleKeyedPatternEventTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(1, "middle", 2.0), 1L),
			Tuple2.of(new Event(2, "middle", 2.0), 4L),
			Tuple2.of(new Event(2, "start", 2.0), 3L),
			Tuple2.of(new Event(1, "end", 3.0), 3L),
			Tuple2.of(new Event(3, "start", 4.1), 5L),
			Tuple2.of(new Event(1, "end", 4.0), 10L),
			Tuple2.of(new Event(2, "end", 2.0), 8L),
			Tuple2.of(new Event(1, "middle", 5.0), 7L),
			Tuple2.of(new Event(3, "middle", 6.0), 9L),
			Tuple2.of(new Event(3, "end", 7.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(3, "end", 7.0), 100L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event,Long>>() {

			@Override
			public long extractTimestamp(Tuple2<Event, Long> element, long currentTimestamp) {
				return element.f1;
			}

			@Override
			public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.f1 - 5);
			}

		}).map(new MapFunction<Tuple2<Event, Long>, Event>() {

			@Override
			public Event map(Tuple2<Event, Long> value) throws Exception {
				return value.f0;
			}
		}).keyBy(new KeySelector<Event, Integer>() {

			@Override
			public Integer getKey(Event value) throws Exception {
				return value.getId();
			}
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedBy("middle").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedBy("end").where(new FilterFunction<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, Event> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").getId()).append(",")
						.append(pattern.get("middle").getId()).append(",")
						.append(pattern.get("end").getId());

					return builder.toString();
				}
			}
		);

		result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

		// the expected sequences of matching event ids
		expected = "1,1,1\n2,2,2";

		env.execute();
	}
}
