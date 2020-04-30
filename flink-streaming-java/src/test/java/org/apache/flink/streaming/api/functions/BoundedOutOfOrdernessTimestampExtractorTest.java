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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BoundedOutOfOrdernessTimestampExtractor}.
 */
public class BoundedOutOfOrdernessTimestampExtractorTest {

	@Test
	public void testInitializationAndRuntime() {
		Time maxAllowedLateness = Time.milliseconds(10L);
		BoundedOutOfOrdernessTimestampExtractor<Long> extractor =
			new LongExtractor(maxAllowedLateness);

		assertEquals(maxAllowedLateness.toMilliseconds(),
			extractor.getMaxOutOfOrdernessInMillis());

		runValidTests(extractor);
	}

	@Test
	public void testInitialFinalAndWatermarkUnderflow() {
		BoundedOutOfOrdernessTimestampExtractor<Long> extractor = new LongExtractor(Time.milliseconds(10L));
		assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

		extractor.extractTimestamp(Long.MIN_VALUE, -1L);

		// the following two lines check for underflow.
		// We have a max latency of 5 millis.
		// We insert an element with ts of Long.MIN_VALUE + 2, which will now be the max ts,
		// then when getting the next watermark, we would have Long.MIN_VALUE + 2 - 5 which
		// would lead to underflow.

		extractor.extractTimestamp(Long.MIN_VALUE + 2, -1);
		assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

		extractor.extractTimestamp(Long.MAX_VALUE, -1L);
		assertEquals(Long.MAX_VALUE - 10, extractor.getCurrentWatermark().getTimestamp());
	}

	// ------------------------------------------------------------------------

	private void runValidTests(BoundedOutOfOrdernessTimestampExtractor<Long> extractor) {
		assertEquals(new Watermark(Long.MIN_VALUE), extractor.getCurrentWatermark());

		assertEquals(13L, extractor.extractTimestamp(13L, 0L));
		assertEquals(13L, extractor.extractTimestamp(13L, 0L));
		assertEquals(14L, extractor.extractTimestamp(14L, 0L));
		assertEquals(20L, extractor.extractTimestamp(20L, 0L));

		assertEquals(new Watermark(10L), extractor.getCurrentWatermark());

		assertEquals(20L, extractor.extractTimestamp(20L, 0L));
		assertEquals(20L, extractor.extractTimestamp(20L, 0L));
		assertEquals(500L, extractor.extractTimestamp(500L, 0L));

		assertEquals(new Watermark(490L), extractor.getCurrentWatermark());

		assertEquals(Long.MAX_VALUE - 1, extractor.extractTimestamp(Long.MAX_VALUE - 1, 0L));
		assertEquals(new Watermark(Long.MAX_VALUE - 11), extractor.getCurrentWatermark());
	}

	// ------------------------------------------------------------------------

	private static class LongExtractor extends BoundedOutOfOrdernessTimestampExtractor<Long> {
		private static final long serialVersionUID = 1L;

		public LongExtractor(Time maxAllowedLateness) {
			super(maxAllowedLateness);
		}

		@Override
		public long extractTimestamp(Long element) {
			return element;
		}
	}
}
