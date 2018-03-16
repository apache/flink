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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link IngestionTimeExtractor}.
 */
public class IngestionTimeExtractorTest {

	@Test
	public void testMonotonousTimestamps() {
		AssignerWithPeriodicWatermarks<String> assigner = new IngestionTimeExtractor<>();

		long maxRecordSoFar = 0L;
		long maxWatermarkSoFar = 0L;

		for (int i = 0; i < 1343; i++) {
			if (i % 7 == 1) {
				Watermark mark = assigner.getCurrentWatermark();
				assertNotNull(mark);

				// increasing watermarks
				assertTrue(mark.getTimestamp() >= maxWatermarkSoFar);
				maxWatermarkSoFar = mark.getTimestamp();

				// tight watermarks
				assertTrue(mark.getTimestamp() >= maxRecordSoFar - 1);
			} else {
				long next = assigner.extractTimestamp("a", Long.MIN_VALUE);

				// increasing timestamps
				assertTrue(next >= maxRecordSoFar);

				// timestamps are never below or at the watermark
				assertTrue(next > maxWatermarkSoFar);

				maxRecordSoFar = next;
			}

			if (i % 9 == 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException ignored) {}
			}
		}
	}
}
