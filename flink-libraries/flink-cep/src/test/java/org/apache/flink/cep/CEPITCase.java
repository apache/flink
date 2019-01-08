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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Either;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * End to end tests of both CEP operators and {@link NFA}.
 */
@SuppressWarnings("serial")
public class CEPITCase extends AbstractTestBase {

	/**
	 * Checks that a certain event sequence is recognized.
	 *
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
		.followedByAny("middle").subtype(SubEvent.class).where(
				new SimpleCondition<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
		.followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).flatSelect((p, o) -> {
			StringBuilder builder = new StringBuilder();

			builder.append(p.get("start").get(0).getId()).append(",")
				.append(p.get("middle").get(0).getId()).append(",")
				.append(p.get("end").get(0).getId());

			o.collect(builder.toString());
		}, Types.STRING);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList("2,6,8"), resultList);
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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		})
			.followedByAny("middle").subtype(SubEvent.class).where(
				new SimpleCondition<SubEvent>() {

					@Override
					public boolean filter(SubEvent value) throws Exception {
						return value.getName().equals("middle");
					}
				}
			)
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(p -> {
			StringBuilder builder = new StringBuilder();

			builder.append(p.get("start").get(0).getId()).append(",")
				.append(p.get("middle").get(0).getId()).append(",")
				.append(p.get("end").get(0).getId());

			return builder.toString();
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("2,2,2", "3,3,3", "42,42,42"), resultList);
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
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("1,5,4"), resultList);
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
			Tuple2.of(new Event(3, "end", 7.0), 7L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

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

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(String::compareTo);

		assertEquals(Arrays.asList("1,1,1", "2,2,2"), resultList);
	}

	@Test
	public void testSimplePatternWithSingleState() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, Integer>> input = env.fromElements(
			new Tuple2<>(0, 1),
			new Tuple2<>(0, 2));

		Pattern<Tuple2<Integer, Integer>, ?> pattern =
			Pattern.<Tuple2<Integer, Integer>>begin("start")
				.where(new SimpleCondition<Tuple2<Integer, Integer>>() {
					@Override
					public boolean filter(Tuple2<Integer, Integer> rec) throws Exception {
						return rec.f1 == 1;
					}
				});

		PatternStream<Tuple2<Integer, Integer>> pStream = CEP.pattern(input, pattern);

		DataStream<Tuple2<Integer, Integer>> result = pStream.select(new PatternSelectFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> select(Map<String, List<Tuple2<Integer, Integer>>> pattern) throws Exception {
				return pattern.get("start").get(0);
			}
		});

		List<Tuple2<Integer, Integer>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList(new Tuple2<>(0, 1)), resultList);
	}

	@Test
	public void testProcessingTimeWithWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Integer> input = env.fromElements(1, 2);

		Pattern<Integer, ?> pattern = Pattern.<Integer>begin("start").followedByAny("end").within(Time.days(1));

		DataStream<Integer> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Integer, Integer>() {
			@Override
			public Integer select(Map<String, List<Integer>> pattern) throws Exception {
				return pattern.get("start").get(0) + pattern.get("end").get(0);
			}
		});

		List<Integer> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		assertEquals(Arrays.asList(3), resultList);
	}

	@Test
	public void testTimeoutHandling() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 1L),
			Tuple2.of(new Event(1, "middle", 2.0), 5L),
			Tuple2.of(new Event(1, "start", 2.0), 4L),
			Tuple2.of(new Event(1, "end", 2.0), 6L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

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
		});

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		}).within(Time.milliseconds(3));

		DataStream<Either<String, String>> result = CEP.pattern(input, pattern).select(
			new PatternTimeoutFunction<Event, String>() {
				@Override
				public String timeout(Map<String, List<Event>> pattern, long timeoutTimestamp) throws Exception {
					return pattern.get("start").get(0).getPrice() + "";
				}
			},
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getPrice()).append(",")
						.append(pattern.get("middle").get(0).getPrice()).append(",")
						.append(pattern.get("end").get(0).getPrice());

					return builder.toString();
				}
			}
		);

		List<Either<String, String>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(Comparator.comparing(either -> either.toString()));

		List<Either<String, String>> expected = Arrays.asList(
			Either.Left.of("1.0"),
			Either.Left.of("2.0"),
			Either.Left.of("2.0"),
			Either.Right.of("2.0,2.0,2.0")
		);

		assertEquals(expected, resultList);
	}

	/**
	 * Checks that a certain event sequence is recognized with an OR filter.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSimpleOrFilterPatternCEP() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(
			new Event(1, "start", 1.0),
			new Event(2, "middle", 2.0),
			new Event(3, "end", 3.0),
			new Event(4, "start", 4.0),
			new Event(5, "middle", 5.0),
			new Event(6, "end", 6.0)
		);

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
			.where(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("start");
				}
			})
			.followedByAny("middle")
			.where(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getPrice() == 2.0;
				}
			})
			.or(new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event value) throws Exception {
					return value.getPrice() == 5.0;
				}
			})
			.followedByAny("end").where(new SimpleCondition<Event>() {

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("end");
				}
			});

		DataStream<String> result = CEP.pattern(input, pattern).select(new PatternSelectFunction<Event, String>() {

			@Override
			public String select(Map<String, List<Event>> pattern) {
				StringBuilder builder = new StringBuilder();

				builder.append(pattern.get("start").get(0).getId()).append(",")
					.append(pattern.get("middle").get(0).getId()).append(",")
					.append(pattern.get("end").get(0).getId());

				return builder.toString();
			}
		});

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		List<String> expected = Arrays.asList(
			"1,5,6",
			"1,2,3",
			"4,5,6",
			"1,2,6"
		);

		expected.sort(String::compareTo);

		resultList.sort(String::compareTo);

		assertEquals(expected, resultList);
	}

	/**
	 * Checks that a certain event sequence is recognized.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSimplePatternEventTimeWithComparator() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// (Event, timestamp)
		DataStream<Event> input = env.fromElements(
			Tuple2.of(new Event(1, "start", 1.0), 5L),
			Tuple2.of(new Event(2, "middle", 2.0), 1L),
			Tuple2.of(new Event(3, "end", 3.0), 3L),
			Tuple2.of(new Event(4, "end", 4.0), 10L),
			Tuple2.of(new Event(5, "middle", 6.0), 7L),
			Tuple2.of(new Event(6, "middle", 5.0), 7L),
			// last element for high final watermark
			Tuple2.of(new Event(7, "middle", 5.0), 100L)
		).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {

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

		EventComparator<Event> comparator = new CustomEventComparator();

		Pattern<Event, ? extends Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("start");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("middle");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("end");
			}
		});

		DataStream<String> result = CEP.pattern(input, pattern, comparator).select(
			new PatternSelectFunction<Event, String>() {

				@Override
				public String select(Map<String, List<Event>> pattern) {
					StringBuilder builder = new StringBuilder();

					builder.append(pattern.get("start").get(0).getId()).append(",")
						.append(pattern.get("middle").get(0).getId()).append(",")
						.append(pattern.get("end").get(0).getId());

					return builder.toString();
				}
			}
		);

		List<String> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		List<String> expected = Arrays.asList(
			"1,6,4",
			"1,5,4"
		);

		expected.sort(String::compareTo);

		resultList.sort(String::compareTo);

		assertEquals(expected, resultList);
	}

	private static class CustomEventComparator implements EventComparator<Event> {
		@Override
		public int compare(Event o1, Event o2) {
			return Double.compare(o1.getPrice(), o2.getPrice());
		}
	}

	@Test
	public void testSimpleAfterMatchSkip() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, String>> input = env.fromElements(
			new Tuple2<>(1, "a"),
			new Tuple2<>(2, "a"),
			new Tuple2<>(3, "a"),
			new Tuple2<>(4, "a"));

		Pattern<Tuple2<Integer, String>, ?> pattern =
			Pattern.<Tuple2<Integer, String>>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
				.where(new SimpleCondition<Tuple2<Integer, String>>() {
					@Override
					public boolean filter(Tuple2<Integer, String> rec) throws Exception {
						return rec.f1.equals("a");
					}
				}).times(2);

		PatternStream<Tuple2<Integer, String>> pStream = CEP.pattern(input, pattern);

		DataStream<Tuple2<Integer, String>> result = pStream.select(new PatternSelectFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
			@Override
			public Tuple2<Integer, String> select(Map<String, List<Tuple2<Integer, String>>> pattern) throws Exception {
				return pattern.get("start").get(0);
			}
		});

		List<Tuple2<Integer, String>> resultList = new ArrayList<>();

		DataStreamUtils.collect(result).forEachRemaining(resultList::add);

		resultList.sort(Comparator.comparing(tuple2 -> tuple2.toString()));

		List<Tuple2<Integer, String>> expected = Arrays.asList(Tuple2.of(1, "a"), Tuple2.of(3, "a"));

		assertEquals(expected, resultList);
	}
}
