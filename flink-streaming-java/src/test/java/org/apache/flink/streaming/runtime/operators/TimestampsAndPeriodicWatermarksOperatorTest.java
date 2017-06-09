/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TimestampsAndPeriodicWatermarksOperator}.
 */
public class TimestampsAndPeriodicWatermarksOperatorTest {

	@Test
	public void testTimestampsAndPeriodicWatermarksOperator() throws Exception {

		final TimestampsAndPeriodicWatermarksOperator<Long> operator =
				new TimestampsAndPeriodicWatermarksOperator<Long>(new LongExtractor());

		OneInputStreamOperatorTestHarness<Long, Long> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

		long currentTime = 0;

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(1L, 1));
		testHarness.processElement(new StreamRecord<>(2L, 1));
		testHarness.processWatermark(new Watermark(2)); // this watermark should be ignored
		testHarness.processElement(new StreamRecord<>(3L, 3));
		testHarness.processElement(new StreamRecord<>(4L, 3));

		// validate first part of the sequence. we poll elements until our
		// watermark updates to "3", which must be the result of the "4" element.
		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long nextElementValue = 1L;
			long lastWatermark = -1L;

			while (lastWatermark < 3) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, nextElementValue, lastWatermark);
					nextElementValue = update.f0;
					lastWatermark = update.f1;

					// check the invariant
					assertTrue(lastWatermark < nextElementValue);
				} else {
					currentTime = currentTime + 10;
					testHarness.setProcessingTime(currentTime);
				}
			}

			output.clear();
		}

		testHarness.processElement(new StreamRecord<>(4L, 4));
		testHarness.processElement(new StreamRecord<>(5L, 4));
		testHarness.processElement(new StreamRecord<>(6L, 4));
		testHarness.processElement(new StreamRecord<>(7L, 4));
		testHarness.processElement(new StreamRecord<>(8L, 4));

		// validate the next part of the sequence. we poll elements until our
		// watermark updates to "7", which must be the result of the "8" element.
		{
			ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
			long nextElementValue = 4L;
			long lastWatermark = 2L;

			while (lastWatermark < 7) {
				if (output.size() > 0) {
					Object next = output.poll();
					assertNotNull(next);
					Tuple2<Long, Long> update = validateElement(next, nextElementValue, lastWatermark);
					nextElementValue = update.f0;
					lastWatermark = update.f1;

					// check the invariant
					assertTrue(lastWatermark < nextElementValue);
				} else {
					currentTime = currentTime + 10;
					testHarness.setProcessingTime(currentTime);
				}
			}

			output.clear();
		}

		testHarness.processWatermark(new Watermark(Long.MAX_VALUE));
		assertEquals(Long.MAX_VALUE, ((Watermark) testHarness.getOutput().poll()).getTimestamp());
	}

	@Test
	public void testNegativeTimestamps() throws Exception {

		final AssignerWithPeriodicWatermarks<Long> assigner = new NeverWatermarkExtractor();

		final TimestampsAndPeriodicWatermarksOperator<Long> operator =
				new TimestampsAndPeriodicWatermarksOperator<Long>(assigner);

		OneInputStreamOperatorTestHarness<Long, Long> testHarness =
				new OneInputStreamOperatorTestHarness<Long, Long>(operator);

		testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

		testHarness.open();

		long[] values = { Long.MIN_VALUE, -1L, 0L, 1L, 2L, 3L, Long.MAX_VALUE };

		for (long value : values) {
			testHarness.processElement(new StreamRecord<>(value));
		}

		ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

		for (long value: values) {
			assertEquals(value, ((StreamRecord<?>) output.poll()).getTimestamp());
		}
	}

	// ------------------------------------------------------------------------

	private Tuple2<Long, Long> validateElement(Object element, long nextElementValue, long currentWatermark) {
		if (element instanceof StreamRecord) {
			@SuppressWarnings("unchecked")
			StreamRecord<Long> record = (StreamRecord<Long>) element;
			assertEquals(nextElementValue, record.getValue().longValue());
			assertEquals(nextElementValue, record.getTimestamp());
			return new Tuple2<>(nextElementValue + 1, currentWatermark);
		}
		else if (element instanceof Watermark) {
			long wt = ((Watermark) element).getTimestamp();
			assertTrue(wt > currentWatermark);
			return new Tuple2<>(nextElementValue, wt);
		}
		else {
			throw new IllegalArgumentException("unrecognized element: " + element);
		}
	}

	// ------------------------------------------------------------------------

	private static class LongExtractor implements AssignerWithPeriodicWatermarks<Long> {
		private static final long serialVersionUID = 1L;

		private long currentTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			currentTimestamp = element;
			return element;
		}

		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
		}
	}

	private static class NeverWatermarkExtractor implements AssignerWithPeriodicWatermarks<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(Long element, long previousElementTimestamp) {
			return element;
		}

		@Override
		public Watermark getCurrentWatermark() {
			return null;
		}
	}
}
