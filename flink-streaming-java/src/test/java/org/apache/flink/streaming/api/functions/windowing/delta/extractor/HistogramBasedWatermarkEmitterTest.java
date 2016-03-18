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
			new LongExtractor(Time.seconds(2), Time.seconds(10), 0.9);

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

	private void testStateTransitions(HistogramBasedWatermarkEmitter<Long> extractor, long initTs) throws Exception {
		assertEquals(new Watermark(Long.MIN_VALUE), extractor.getCurrentWatermark());

		extractor.open(null);										// INITIALIZE

		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(10000L, extractor.extractTimestamp(10000L, 0L));	//10s
		assertEquals(1000L, extractor.extractTimestamp(1000L, 0L));		//0s late
		assertEquals(1000L, extractor.extractTimestamp(1000L, 0L));		//0s late
		assertEquals(30L, extractor.extractTimestamp(30L, 0L));			//0s late
		assertEquals(11000L, extractor.extractTimestamp(11000L, 0L));	//11s
		assertEquals(11000L, extractor.extractTimestamp(11000L, 0L));	//11s

		// watermark before the end of the sampling (so latency = 0)
		assertEquals(new Watermark(11000L), extractor.getCurrentWatermark());

		extractor.trigger(2000);										// TRIGGER

		assertEquals(12000L, extractor.extractTimestamp(12000L, 0L));	//16s

		// watermark after the end of the  sampling period
		long expectedLatency = ((10000 - 1000) >>> 10) << 10;
		assertEquals(new Watermark(12000 - expectedLatency), extractor.getCurrentWatermark());

		// watermark in the next sampling period
		extractor.trigger(12000L);										// TRIGGER

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

		// we are still before the end of the sampling so the allowed lateness is
		// not updated yet.
		assertEquals(new Watermark(14000 - expectedLatency), extractor.getCurrentWatermark());

		// watermark after the end of the second sampling
		extractor.trigger(14000L);										// TRIGGER

		// no elements were late so now the allowed lateness is updated to 0
		assertEquals(new Watermark(14000), extractor.getCurrentWatermark());

		extractor.trigger(24000);										// TRIGGER

		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(35000L, extractor.extractTimestamp(35000L, 0L));	//35s
		assertEquals(33000L, extractor.extractTimestamp(33000L, 0L));	//33s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(32000L, extractor.extractTimestamp(32000L, 0L));	//32s late
		assertEquals(40000L, extractor.extractTimestamp(40000L, 0L));	//35s

		extractor.trigger(26000);										// TRIGGER

		// this is the new latness after the new sampling period
		expectedLatency = ((35000 - 32000) >>> 10) << 10;
		assertEquals(new Watermark(40000 - expectedLatency), extractor.getCurrentWatermark());

		// now it reinitializes and forgets the previous sampling as we jumped from
		// sampling to sampling period.

		extractor.trigger(36000);										// TRIGGER

		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late
		assertEquals(38000L, extractor.extractTimestamp(38000L, 0L));	//38s late

		extractor.trigger(38000);										// TRIGGER

		// because the maximum seen timestamp is not forgotten between sampling periods
		expectedLatency = ((40000 - 38000) >>> 10) << 10;
		assertEquals(new Watermark(40000 - expectedLatency), extractor.getCurrentWatermark());
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
