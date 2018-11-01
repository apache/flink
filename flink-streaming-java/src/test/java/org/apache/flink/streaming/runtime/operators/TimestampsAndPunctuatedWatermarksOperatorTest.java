/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TimestampsAndPunctuatedWatermarksOperator}.
 */
public class TimestampsAndPunctuatedWatermarksOperatorTest {

	@Test
	@SuppressWarnings("unchecked")
	public void testTimestampsAndPeriodicWatermarksOperator() throws Exception {

		final TimestampsAndPunctuatedWatermarksOperator<Tuple2<Long, Boolean>> operator =
				new TimestampsAndPunctuatedWatermarksOperator<>(new PunctuatedExtractor());

		OneInputStreamOperatorTestHarness<Tuple2<Long, Boolean>, Tuple2<Long, Boolean>> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>(3L, true), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(5L, false), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(4L, false), 0L));
		testHarness.processWatermark(new Watermark(10)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(4L, false), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(4L, true), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(9L, false), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(5L, false), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(7L, true), 0L));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>(10L, false), 0L));

		testHarness.processWatermark(new Watermark(Long.MAX_VALUE));

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		// poll the watermark from initializing state.
		output.poll();

		assertEquals(3L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(3L, ((Watermark) output.poll()).getTimestamp());

		assertEquals(5L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(4L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(4L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(4L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(4L, ((Watermark) output.poll()).getTimestamp());

		assertEquals(9L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(5L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(7L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(7L, ((Watermark) output.poll()).getTimestamp());

		assertEquals(10L, ((StreamRecord<Tuple2<Long, Boolean>>) output.poll()).getTimestamp());
		assertEquals(Long.MAX_VALUE, ((Watermark) output.poll()).getTimestamp());
	}

	@Test
	public void testZeroOnNegativeTimestamps() throws Exception {

		final AssignerWithPunctuatedWatermarks<Long> assigner = new NeverWatermarkExtractor();

		final TimestampsAndPunctuatedWatermarksOperator<Long> operator =
				new TimestampsAndPunctuatedWatermarksOperator<Long>(assigner);

		OneInputStreamOperatorTestHarness<Long, Long> testHarness =
				new OneInputStreamOperatorTestHarness<Long, Long>(operator);

		testHarness.open();

		long[] values = { Long.MIN_VALUE, -1L, 0L, 1L, 2L, 3L, Long.MAX_VALUE };

		for (long value : values) {
			testHarness.processElement(new StreamRecord<>(value));
		}

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		// poll the watermark from initializing state.
		output.poll();

		for (long value: values) {
			assertEquals(value, ((StreamRecord<?>) output.poll()).getTimestamp());
		}
	}

	// ------------------------------------------------------------------------

	private static class PunctuatedExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<Long, Boolean>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(Tuple2<Long, Boolean> element, long previousTimestamp) {
			return element.f0;
		}

		@Override
		public Watermark checkAndGetNextWatermark(Tuple2<Long, Boolean> lastElement, long extractedTimestamp) {
			return lastElement.f1 ? new Watermark(extractedTimestamp) : null;
		}
	}

	private static class NeverWatermarkExtractor implements AssignerWithPunctuatedWatermarks<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			return element;
		}

		@Override
		public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
			return null;
		}
	}
}
