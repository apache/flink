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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PreAggregationOperator}.
 */
public class PreAggregationOperatorTest extends TestLogger {
	private static final long WINDOW_SIZE = 100;

	@Test
	public void testWatermarkEmit() throws Exception {
		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(false)) {
			testHarness.processElement(record(0, 40), 0);
			testHarness.processElement(record(0, 2), 99);
			assertOutput(testHarness);

			testHarness.processWatermark(99);
			assertOutput(
				testHarness,
				expectedOutput(0, 0, 100, 42));

			assertEquals(99, testHarness.getCurrentWatermark());
		}
	}

	@Test
	public void testPreAggregate() throws Exception {
		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(false)) {
			testHarness.processElement(record(0, 10), 0);
			testHarness.processElement(record(0, 20), 10);
			testHarness.processElement(record(1, 10), 10);
			testHarness.processElement(record(0, 12), 50);
			testHarness.processElement(record(1, 3), 50);
			testHarness.processElement(record(0, 2), 110);
			testHarness.processElement(record(0, 12), 120);
			testHarness.processElement(record(0, 30), 150);

			assertOutput(testHarness);

			testHarness.processWatermark(90);
			assertOutput(testHarness);

			testHarness.processWatermark(110);
			assertOutput(
				testHarness,
				expectedOutput(0, 0, 100, 42),
				expectedOutput(1, 0, 100, 13));

			testHarness.processWatermark(210);
			assertOutput(
				testHarness,
				expectedOutput(0, 100, 200, 44));
		}
	}

	@Test
	public void testPreAggregateAndFlushAllOnWatermark() throws Exception {
		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(true)) {
			testHarness.processElement(record(0, 10), 0);
			testHarness.processElement(record(0, 20), 10);
			testHarness.processElement(record(1, 10), 10);
			testHarness.processElement(record(0, 12), 50);
			testHarness.processElement(record(1, 3), 50);
			testHarness.processElement(record(0, 2), 110);
			testHarness.processElement(record(0, 12), 120);
			testHarness.processElement(record(0, 30), 150);

			assertOutput(testHarness);

			testHarness.processWatermark(90);
			assertOutput(
				testHarness,
				expectedOutput(0, 0, 100, 42),
				expectedOutput(0, 100, 200, 44),
				expectedOutput(1, 0, 100, 13));
			assertOutput(testHarness);
		}
	}

	@Test
	public void testCheckpoint() throws Exception {
		OperatorStateHandles snapshot1;
		OperatorStateHandles snapshot2;

		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(true)) {

			testHarness.processElement(record(0, 40), 0);
			testHarness.processElement(record(0, 2), 10);
			testHarness.processElement(record(1, 10), 10);
			testHarness.processElement(record(1, 3), 50);

			snapshot1 = testHarness.snapshot(0, 200);

			testHarness.processElement(record(0, 44), 150);

			snapshot2 = testHarness.snapshot(1, 300);
		}

		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(true, snapshot1)) {

			assertOutput(testHarness);
			testHarness.processWatermark(90);
			assertOutput(
				testHarness,
				expectedOutput(0, 0, 100, 42),
				expectedOutput(1, 0, 100, 13));
		}

		try (OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
				createTestHarness(true, snapshot2)) {

			assertOutput(testHarness);
			testHarness.processWatermark(90);
			assertOutput(
				testHarness,
				expectedOutput(0, 0, 100, 42),
				expectedOutput(0, 100, 200, 44),
				expectedOutput(1, 0, 100, 13));
		}
	}

	private static void assertOutput(
			OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness,
			StreamRecord<?>... records) {
		List<?> actualOutput = extractOutput(testHarness);
		testHarness.getOutput().clear();

		assertEquals(
			String.format("Expected %d elements, but got %s", records.length, actualOutput),
			records.length,
			actualOutput.size());

		for (int i = 0; i < records.length; i++) {
			assertEquals(records[i], actualOutput.get(i));
		}
	}

	private static List<StreamRecord<? extends Tuple3<Integer, TimeWindow, LongAccumulator>>> extractOutput(
			OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness) {

		return testHarness.extractOutputStreamRecords().stream()
			.sorted((leftRecord, rightRecord) -> {
				Tuple3<Integer, TimeWindow, LongAccumulator> left = leftRecord.getValue();
				Tuple3<Integer, TimeWindow, LongAccumulator> right = rightRecord.getValue();
				int keyCompare = left.f0.compareTo(right.f0);
				if (keyCompare != 0) {
					return keyCompare;
				}
				return Long.compare(left.f1.getStart(), right.f1.getStart());
			})
			.collect(Collectors.toList());
	}

	private static Record record(int key, long value) {
		return new Record(key, value);
	}

	private static class Record {
		public final int key;
		public final long value;

		public Record() {
			this(0, 0L);
		}

		public Record(int key, long value) {
			this.key = key;
			this.value = value;
		}
	}

	private static class LongAccumulator {
		public long value;

		public LongAccumulator(long value) {
			this.value = value;
		}

		@Override
		public int hashCode() {
			return Long.hashCode(value);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof LongAccumulator)) {
				return false;
			}
			LongAccumulator other = (LongAccumulator) obj;
			return value == other.value;
		}
	}

	private static class RecordSumPreAggregation
		implements AggregateFunction<Record, LongAccumulator, Void>, Serializable {
		@Override
		public LongAccumulator createAccumulator() {
			return new LongAccumulator(0);
		}

		@Override
		public LongAccumulator add(Record value, LongAccumulator accumulator) {
			accumulator.value += value.value;
			return accumulator;
		}

		@Override
		public Void getResult(LongAccumulator accumulator) {
			throw new UnsupportedOperationException();
		}

		@Override
		public LongAccumulator merge(LongAccumulator a, LongAccumulator b) {
			return new LongAccumulator(a.value + b.value);
		}
	}

	private OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> createTestHarness(
			boolean flushAllOnWatermark) throws Exception {
		return createTestHarness(flushAllOnWatermark, Optional.empty());
	}

	private OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> createTestHarness(
			boolean flushAllOnWatermark,
			OperatorStateHandles snapshot) throws Exception {
		return createTestHarness(flushAllOnWatermark, Optional.of(snapshot));
	}

	private OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> createTestHarness(
			boolean flushAllOnWatermark,
			Optional<OperatorStateHandles> snapshot) throws Exception {

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		PreAggregationOperator<Integer, Record, LongAccumulator, TimeWindow> preAggregationOperator = new PreAggregationOperator<>(
			new RecordSumPreAggregation(),
			record -> record.key,
			TypeInformation.of(Integer.class),
			TypeInformation.of(LongAccumulator.class),
			windowAssigner,
			flushAllOnWatermark);

		OneInputStreamOperatorTestHarness<Record, Tuple3<Integer, TimeWindow, LongAccumulator>> testHarness =
			new OneInputStreamOperatorTestHarness<>(preAggregationOperator);
		testHarness.setup();
		if (snapshot.isPresent()) {
			testHarness.initializeState(snapshot.get());
		}
		testHarness.open();
		return testHarness;
	}

	private static StreamRecord<Tuple3<Integer, TimeWindow, LongAccumulator>> expectedOutput(
			int key,
			long windowStart,
			long windowEnd,
			long value) {
		TimeWindow timeWindow = new TimeWindow(windowStart, windowEnd);
		return new StreamRecord<>(
			Tuple3.of(key, timeWindow, new LongAccumulator(value)),
			timeWindow.maxTimestamp());
	}
}
