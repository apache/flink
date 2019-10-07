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

package org.apache.flink.metrics.signalfx;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.signalfx.metrics.flush.AggregateMetricSender;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the ScheduledSignalFXReporterTest.
 */
public class SignalFXReporterTest {

	/**
	 * This test verifies that metrics are properly added and removed to/from the ScheduledSignalFXReporter and
	 * the underlying SignalFX MetricRegistry.
	 */
	@Test
	public void testMetricCleanup() {
		TestingSignalFXReporter rep = new TestingSignalFXReporter(null, "");

		MetricGroup mp = new UnregisteredMetricsGroup();

		Counter c = new SimpleCounter();
		Meter m = new Meter() {
			@Override
			public void markEvent() {
			}

			@Override
			public void markEvent(long n) {
			}

			@Override
			public double getRate() {
				return 0;
			}

			@Override
			public long getCount() {
				return 0;
			}
		};
		Gauge g = new Gauge() {
			@Override
			public Object getValue() {
				return 0;
			}
		};

		rep.notifyOfAddedMetric(c, "counter", mp);
		assertEquals(1, rep.getCounters().size());

		rep.notifyOfAddedMetric(m, "meter", mp);
		assertEquals(1, rep.getMeters().size());

		rep.notifyOfAddedMetric(g, "gauge", mp);
		assertEquals(1, rep.getGauges().size());

		rep.notifyOfRemovedMetric(c, "counter", mp);
		assertEquals(0, rep.getCounters().size());

		rep.notifyOfRemovedMetric(m, "meter", mp);
		assertEquals(0, rep.getMeters().size());

		rep.notifyOfRemovedMetric(g, "gauge", mp);
		assertEquals(0, rep.getGauges().size());
	}

	/**
	 * Dummy test reporter.
	 */
	public static class TestingSignalFXReporter extends SignalFXReporter {

		public TestingSignalFXReporter(String token, String filters) {
			super(token, filters);
		}

		@Override
		public AggregateMetricSender getReporter() {
			return null;
		}

		@Override
		public void close() {
			// don't do anything
		}
	}
}
