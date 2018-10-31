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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;

import org.influxdb.dto.Point;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link MetricMapper} checking that metrics are converted to InfluxDB client objects as expected.
 */
public class MetricMapperTest {

	private final String name = "a-metric-name";
	private final MeasurementInfo info = getMeasurementInfo(name);
	private final Instant timestamp = Instant.now();

	@Test
	public void testMapGauge() {
		verifyPoint(
			MetricMapper.map(info, timestamp, (Gauge<Number>) () -> 42),
			"value=42");

		verifyPoint(
			MetricMapper.map(info, timestamp, (Gauge<Number>) () -> null),
			"value=null");

		verifyPoint(
			MetricMapper.map(info, timestamp, (Gauge<String>) () -> "hello"),
			"value=hello");

		verifyPoint(
			MetricMapper.map(info, timestamp, (Gauge<Long>) () -> 42L),
			"value=42");
	}

	@Test
	public void testMapCounter() {
		Counter counter = mock(Counter.class);
		when(counter.getCount()).thenReturn(42L);

		verifyPoint(
			MetricMapper.map(info, timestamp, counter),
			"count=42");
	}

	@Test
	public void testMapHistogram() {
		HistogramStatistics statistics = mock(HistogramStatistics.class);
		when(statistics.getMax()).thenReturn(-5L);
		when(statistics.getMin()).thenReturn(50L);
		when(statistics.getMean()).thenReturn(1.2);
		when(statistics.getStdDev()).thenReturn(0.7);
		when(statistics.getQuantile(.5)).thenReturn(1.0);
		when(statistics.getQuantile(.75)).thenReturn(2.0);
		when(statistics.getQuantile(.95)).thenReturn(3.0);
		when(statistics.getQuantile(.98)).thenReturn(4.0);
		when(statistics.getQuantile(.99)).thenReturn(5.0);
		when(statistics.getQuantile(.999)).thenReturn(6.0);

		Histogram histogram = mock(Histogram.class);
		when(histogram.getStatistics()).thenReturn(statistics);
		when(histogram.getCount()).thenReturn(42L);

		verifyPoint(
			MetricMapper.map(info, timestamp, histogram),
			"50-percentile=1.0",
			"75-percentile=2.0",
			"95-percentile=3.0",
			"98-percentile=4.0",
			"99-percentile=5.0",
			"999-percentile=6.0",
			"count=0",
			"max=-5",
			"mean=1.2",
			"min=50",
			"run-count=42",
			"std-dev=0.7");
	}

	@Test
	public void testMapMeter() {
		Meter meter = mock(Meter.class);
		when(meter.getCount()).thenReturn(42L);
		when(meter.getRate()).thenReturn(2.5);

		verifyPoint(
			MetricMapper.map(info, timestamp, meter),
			"count=42",
			"rate=2.5");
	}

	private void verifyPoint(Point point, String... expectedFields) {
		// Most methods of Point are package private. We use toString() method to check that values are as expected.
		// An alternative can be to call lineProtocol() method, which additionally escapes values for InfluxDB format.
		assertEquals(
			"Point [name=" + name
			+ ", time=" + timestamp.toEpochMilli()
			+ ", tags={tag-1=42, tag-2=green}"
			+ ", precision=MILLISECONDS"
			+ ", fields={" + String.join(", ", expectedFields) + "}"
			+ "]",
			point.toString());
	}

	private static MeasurementInfo getMeasurementInfo(String name) {
		Map<String, String> tags = new HashMap<>();
		tags.put("tag-1", "42");
		tags.put("tag-2", "green");
		return new MeasurementInfo(name, tags);
	}
}
