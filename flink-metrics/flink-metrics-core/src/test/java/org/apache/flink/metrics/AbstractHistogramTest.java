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

package org.apache.flink.metrics;

import org.apache.flink.util.TestLogger;

import static org.junit.Assert.assertEquals;

/**
 * Abstract base class for testing {@link Histogram} and {@link HistogramStatistics} implementations.
 */
public class AbstractHistogramTest extends TestLogger {
	protected void testHistogram(int size, Histogram histogram) {
		HistogramStatistics statistics;

		for (int i = 0; i < size; i++) {
			histogram.update(i);

			statistics = histogram.getStatistics();
			assertEquals(i + 1, histogram.getCount());
			assertEquals(histogram.getCount(), statistics.size());
			assertEquals(i, statistics.getMax());
			assertEquals(0, statistics.getMin());
		}

		statistics = histogram.getStatistics();
		assertEquals(size, statistics.size());
		assertEquals((size - 1) / 2.0, statistics.getQuantile(0.5), 0.001);

		for (int i = size; i < 2 * size; i++) {
			histogram.update(i);

			statistics = histogram.getStatistics();
			assertEquals(i + 1, histogram.getCount());
			assertEquals(size, statistics.size());
			assertEquals(i, statistics.getMax());
			assertEquals(i + 1 - size, statistics.getMin());
		}

		statistics = histogram.getStatistics();
		assertEquals(size, statistics.size());
		assertEquals(size + (size - 1) / 2.0, statistics.getQuantile(0.5), 0.001);
	}
}
