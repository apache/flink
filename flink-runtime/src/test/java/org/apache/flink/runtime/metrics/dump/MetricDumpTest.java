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

package org.apache.flink.runtime.metrics.dump;

import org.junit.Test;

import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_COUNTER;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_GAUGE;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_HISTOGRAM;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_METER;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link MetricDump} classes.
 */
public class MetricDumpTest {
	@Test
	public void testDumpedCounter() {
		QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

		MetricDump.CounterDump cd = new MetricDump.CounterDump(info, "counter", 4);

		assertEquals("counter", cd.name);
		assertEquals(4, cd.count);
		assertEquals(info, cd.scopeInfo);
		assertEquals(METRIC_CATEGORY_COUNTER, cd.getCategory());
	}

	@Test
	public void testDumpedGauge() {
		QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

		MetricDump.GaugeDump gd = new MetricDump.GaugeDump(info, "gauge", "hello");

		assertEquals("gauge", gd.name);
		assertEquals("hello", gd.value);
		assertEquals(info, gd.scopeInfo);
		assertEquals(METRIC_CATEGORY_GAUGE, gd.getCategory());
	}

	@Test
	public void testDumpedHistogram() {
		QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

		MetricDump.HistogramDump hd = new MetricDump.HistogramDump(info, "hist", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

		assertEquals("hist", hd.name);
		assertEquals(1, hd.min);
		assertEquals(2, hd.max);
		assertEquals(3, hd.mean, 0.1);
		assertEquals(4, hd.median, 0.1);
		assertEquals(5, hd.stddev, 0.1);
		assertEquals(6, hd.p75, 0.1);
		assertEquals(7, hd.p90, 0.1);
		assertEquals(8, hd.p95, 0.1);
		assertEquals(9, hd.p98, 0.1);
		assertEquals(10, hd.p99, 0.1);
		assertEquals(11, hd.p999, 0.1);
		assertEquals(info, hd.scopeInfo);
		assertEquals(METRIC_CATEGORY_HISTOGRAM, hd.getCategory());
	}

	@Test
	public void testDumpedMeter() {
		QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

		MetricDump.MeterDump md = new MetricDump.MeterDump(info, "meter", 5.0);

		assertEquals("meter", md.name);
		assertEquals(5.0, md.rate, 0.1);
		assertEquals(info, md.scopeInfo);
		assertEquals(METRIC_CATEGORY_METER, md.getCategory());
	}
}
