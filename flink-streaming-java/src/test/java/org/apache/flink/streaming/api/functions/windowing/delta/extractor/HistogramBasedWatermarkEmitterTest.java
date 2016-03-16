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
package org.apache.flink.streaming.api.functions.windowing.delta.extractor;

import org.apache.flink.streaming.api.functions.HistogramBasedWatermarkEmitter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class HistogramBasedWatermarkEmitterTest {

	@Test
	public void testInitializationAndBehavior() throws Exception {
		HistogramBasedWatermarkEmitter<Long> extractor =
			new LongExtractor(Time.seconds(10), Time.seconds(2), 0.9);

		// setting the testing flag to true through reflection.
		Field testingFlag = HistogramBasedWatermarkEmitter.class.getDeclaredField("testing");
		testingFlag.setAccessible(true);
		testingFlag.setBoolean(extractor, true);
		boolean fieldValue = (Boolean) testingFlag.get(extractor);
		assertEquals(fieldValue, true);

		// test the initialization of the config
		assertEquals(0.9, extractor.getCoveragePercentile(), 0.0000001);
		assertEquals(Time.seconds(10).toMilliseconds(), extractor.getNonSamplingIntervalDurationInMillis());
		assertEquals(Time.seconds(2).toMilliseconds(), extractor.getSamplingPeriodDurationInMillis());

		// tests the runtime behavior
		testStateTransitions(extractor, 0);
	}

	@Test
	public void testInitialAndFinalWatermark() {
		HistogramBasedWatermarkEmitter<Long> extractor = new LongExtractor(Time.seconds(10), Time.seconds(2), 0.9);
		assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

		extractor.extractTimestamp(Long.MAX_VALUE, -1L);
		assertEquals(Long.MAX_VALUE, extractor.getCurrentWatermark().getTimestamp());
	}

	// ------------------------------------------------------------------------

	private void testStateTransitions(HistogramBasedWatermarkEmitter<Long> extractor, long initTs) {
		assertEquals(new Watermark(Long.MIN_VALUE), extractor.getCurrentWatermark());

		extractor.setCurrentSystemTime(0);

		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(1000L, extractor.extractTimestamp(1000L, 0L));		//0s
		assertEquals(1000L, extractor.extractTimestamp(1000L, 0L));		//0s
		assertEquals(30L, extractor.extractTimestamp(30L, 0L));			//0s
		assertEquals(11000L, extractor.extractTimestamp(11000L, 0L));	//11s
		assertEquals(11000L, extractor.extractTimestamp(11000L, 0L));	//11s

		// watermark before the end of the sampling (so latency = 0)
		assertEquals(new Watermark(11000L), extractor.getCurrentWatermark());

		extractor.setCurrentSystemTime(3000);
		assertEquals(3000L, extractor.getCurrentSystemTime());

		assertEquals(11000L, extractor.extractTimestamp(11000L, 0L));	//16s

		// watermark after the end of the  sampling period, in the non-sampling one
		long expectedLatency = ((10000 - 1000) >>> 10) << 10;
		assertEquals(new Watermark(11000 - expectedLatency), extractor.getCurrentWatermark());

		// watermark in the next sampling period
		extractor.setCurrentSystemTime(12100L);

		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s
		assertEquals(14000L, extractor.extractTimestamp(14000L, 0L));	//14s

		assertEquals(new Watermark(14000 - expectedLatency), extractor.getCurrentWatermark());

		// watermark after the end of the second sampling
		extractor.setCurrentSystemTime(14100L);

		// no elements were late so now expected latency = 0
		assertEquals(16000L, extractor.extractTimestamp(16000L, 0L));	//16s
		assertEquals(new Watermark(16000), extractor.getCurrentWatermark());

		extractor.setCurrentSystemTime(24100);

		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(33000L, extractor.extractTimestamp(33000L, 0L));	//33s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s

		extractor.setCurrentSystemTime(26100);

		// this would be the latency if it were to recompute now.
		expectedLatency = ((35000 - 32000) >>> 10) << 10;
		assertEquals(new Watermark(35000 - expectedLatency), extractor.getCurrentWatermark());

		// now it reinitializes and forgets the previous sampling as we jumped from
		// sampling to sampling period.

		extractor.setCurrentSystemTime(36100);

		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s

		extractor.setCurrentSystemTime(38100);
		assertEquals(new Watermark(38000), extractor.getCurrentWatermark());
	}

	// ------------------------------------------------------------------------

	private static class LongExtractor extends HistogramBasedWatermarkEmitter<Long> {
		private static final long serialVersionUID = 1L;

		public LongExtractor(Time nonSamplingInterval, Time samplingPeriod, double coverage) {
			super(nonSamplingInterval, samplingPeriod, coverage);
		}

		@Override
		public long extractTimestamp(Long element) {
				return element;
			}
	}
}
