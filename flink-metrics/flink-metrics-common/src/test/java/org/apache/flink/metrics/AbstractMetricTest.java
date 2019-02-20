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

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for abstract metrics.
 */
public class AbstractMetricTest {

	@Test
	public void testGauge() {
		MetricDef metricDef = new MetricDef()
			.define("gauge", "A standalone gauge", MetricSpec.gauge());

		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);
		testMetrics.setGauge("gauge", new Gauge<Integer>() {
			int i = 0;
			@Override
			public Integer getValue() {
				return i++;
			}
		});
		// The gauge value should increment by 1 each time getValue() is invoked.
		assertEquals(0, testMetrics.getGauge("gauge").getValue());
		assertEquals(1, testMetrics.getGauge("gauge").getValue());
	}

	@Test
	public void testStandaloneCounter() {
		MetricDef metricDef = new MetricDef()
			.define("counter", "A standalone counter", MetricSpec.counter());
		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);

		// Increment the standalone counter.
		testMetrics.getCounter("counter").inc();
		assertEquals(1, testMetrics.getCounter("counter").getCount());
	}

	@Test
	public void testSubMetricCounter() {
		MetricDef metricDef = new MetricDef()
			.define("counter", "A standalone counter", MetricSpec.counter())
			.define(
				"meter",
				"A meter uses previously defined counter.",
				MetricSpec.meter("counter"));
		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);

		// Increment the sub-metric counter.
		testMetrics.getCounter("counter").inc();
		assertEquals(1, testMetrics.getCounter("counter").getCount());

		// Marking an event should increment the counter to 2.
		testMetrics.getMeter("meter").markEvent();
		assertEquals(2, testMetrics.getMeter("meter").getCount());
	}

	@Test
	public void testHistogram() {
		MetricDef metricDef = new MetricDef()
			.define("histogram", "A histogram", MetricSpec.histogram());
		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);

		testMetrics.getHistogram("histogram").update(1);
		assertEquals(1, testMetrics.getHistogram("histogram").getCount());
	}

	@Test
	public void testInstanceSpec() {
		final Gauge gauge = () -> 100;
		final Meter meter = new MeterView(60);
		final Counter counter = new SimpleCounter();
		final Histogram histogram = new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir()));

		MetricDef metricDef = new MetricDef()
			.define("gauge", "doc", MetricSpec.of(gauge))
			.define("meter", "doc", MetricSpec.of(meter))
			.define("counter", "doc", MetricSpec.of(counter))
			.define("histogram", "doc", MetricSpec.of(histogram));

		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);
		meter.markEvent(100);
		counter.inc(100);
		histogram.update(100);

		assertEquals(100, testMetrics.getGauge("gauge").getValue());
		assertEquals(100, testMetrics.getMeter("meter").getCount());
		assertEquals(100, testMetrics.getCounter("counter").getCount());
		assertEquals(1, testMetrics.getHistogram("histogram").getCount());
	}

	@Test(expected = IllegalStateException.class)
	public void testMissingSubMetricDefinition() {
		MetricDef metricDef = new MetricDef()
			.define("meter", "doc", MetricSpec.meter("non-existing-counter"));
		new TestMetrics(new UnregisteredMetricsGroup(), metricDef);
	}

	@Test(expected = IllegalStateException.class)
	public void testUnsetGauge() {
		MetricDef metricDef = new MetricDef()
			.define("gauge", "A standalone gauge", MetricSpec.gauge());

		TestMetrics testMetrics = new TestMetrics(new UnregisteredMetricsGroup(), metricDef);
		testMetrics.getGauge("gauge").getValue();
	}

	// ------------ Metric class for testing -------------------

	private static class TestMetrics extends AbstractMetrics {

		TestMetrics(MetricGroup metricGroup, MetricDef metricDef) {
			super(metricGroup, metricDef);
		}

	}
}
