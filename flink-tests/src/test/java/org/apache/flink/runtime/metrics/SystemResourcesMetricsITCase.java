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

package org.apache.flink.runtime.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.configuration.MetricOptions.REPORTERS_LIST;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for proper initialization of the system resource metrics.
 */
public class SystemResourcesMetricsITCase extends TestLogger {

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(1)
			.build());

	private static Configuration getConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setBoolean(SYSTEM_RESOURCE_METRICS, true);
		configuration.setString(REPORTERS_LIST, "test_reporter");
		configuration.setString("metrics.reporter.test_reporter.class", TestReporter.class.getName());
		return configuration;
	}

	@Test
	public void startTaskManagerAndCheckForRegisteredSystemMetrics() throws Exception {
		assertEquals(1, TestReporter.OPENED_REPORTERS.size());
		TestReporter reporter = TestReporter.OPENED_REPORTERS.iterator().next();

		List<String> expectedPatterns = getExpectedPatterns();

		Collection<String> gaugeNames = reporter.getGauges().values();

		for (String expectedPattern : expectedPatterns) {
			boolean found = false;
			for (String gaugeName : gaugeNames) {
				if (gaugeName.matches(expectedPattern)) {
					found = true;
				}
			}
			if (!found) {
				fail(String.format("Failed to find gauge [%s] in registered gauges [%s]", expectedPattern, gaugeNames));
			}
		}
	}

	private static List<String> getExpectedPatterns() {
		String[] expectedGauges = new String[] {
			"System.CPU.Idle",
			"System.CPU.Sys",
			"System.CPU.User",
			"System.CPU.IOWait",
			"System.CPU.Irq",
			"System.CPU.SoftIrq",
			"System.CPU.Nice",
			"System.Memory.Available",
			"System.Memory.Total",
			"System.Swap.Used",
			"System.Swap.Total",
			"System.Network.*ReceiveRate",
			"System.Network.*SendRate"
		};

		String[] expectedHosts = new String[] {
			"localhost.taskmanager.([a-f0-9\\\\-])*.",
			"localhost.jobmanager."
		};

		List<String> patterns = new ArrayList<>();
		for (String expectedHost : expectedHosts) {
			for (String expectedGauge : expectedGauges) {
				patterns.add(expectedHost + expectedGauge);
			}
		}
		return patterns;
	}

	/**
	 * Test metric reporter that exposes registered metrics.
	 */
	public static final class TestReporter extends AbstractReporter {
		public static final Set<TestReporter> OPENED_REPORTERS = ConcurrentHashMap.newKeySet();

		@Override
		public String filterCharacters(String input) {
			return input;
		}

		@Override
		public void open(MetricConfig config) {
			OPENED_REPORTERS.add(this);
		}

		@Override
		public void close() {
			OPENED_REPORTERS.remove(this);
		}

		public Map<Gauge<?>, String> getGauges() {
			return gauges;
		}
	}
}
