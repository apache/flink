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
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.util.TestLogger;

import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.influxdb.InfluxDB;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for {@link InfluxdbReporter}.
 */
public class InfluxdbReporterTest extends TestLogger {
	private static final String TEST_INFLUXDB_DB = "test-42";
	private static final String METRIC_HOSTNAME = "task-mgr-1";
	private static final String METRIC_TM_ID = "tm-id-123";

	@Rule
	public final WireMockRule wireMockRule = new WireMockRule(
		wireMockConfig()
			.dynamicPort()
			.notifier(new ConsoleNotifier(false)));

	@Test
	public void testReporterRegistration() throws Exception {
		MetricRegistryImpl metricRegistry = createMetricRegistry(InfluxdbReporterOptions.RETENTION_POLICY.defaultValue(),
			InfluxdbReporterOptions.CONSISTENCY.defaultValue());
		try {
			assertEquals(1, metricRegistry.getReporters().size());
			MetricReporter reporter = metricRegistry.getReporters().get(0);
			assertTrue(reporter instanceof InfluxdbReporter);
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void testMetricRegistration() throws Exception {
		MetricRegistryImpl metricRegistry = createMetricRegistry(InfluxdbReporterOptions.RETENTION_POLICY.defaultValue(),
			InfluxdbReporterOptions.CONSISTENCY.defaultValue());
		try {
			String metricName = "TestCounter";
			Counter counter = registerTestMetric(metricName, metricRegistry);

			InfluxdbReporter reporter = (InfluxdbReporter) metricRegistry.getReporters().get(0);
			MeasurementInfo measurementInfo = reporter.counters.get(counter);
			assertNotNull("test metric must be registered in the reporter", measurementInfo);
			assertEquals("taskmanager_" + metricName, measurementInfo.getName());
			assertThat(measurementInfo.getTags(), hasEntry("host", METRIC_HOSTNAME));
			assertThat(measurementInfo.getTags(), hasEntry("tm_id", METRIC_TM_ID));
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void testMetricReporting() throws Exception {
		String retentionPolicy = "one_hour";
		InfluxDB.ConsistencyLevel consistencyLevel = InfluxDB.ConsistencyLevel.ANY;
		MetricRegistryImpl metricRegistry = createMetricRegistry(retentionPolicy, consistencyLevel);
		try {
			String metricName = "TestCounter";
			Counter counter = registerTestMetric(metricName, metricRegistry);
			counter.inc(42);

			stubFor(post(urlPathEqualTo("/write"))
				.willReturn(aResponse()
					.withStatus(200)));

			InfluxdbReporter reporter = (InfluxdbReporter) metricRegistry.getReporters().get(0);
			reporter.report();

			verify(postRequestedFor(urlPathEqualTo("/write"))
				.withQueryParam("db", equalTo(TEST_INFLUXDB_DB))
				.withQueryParam("rp", equalTo(retentionPolicy))
				.withQueryParam("consistency", equalTo(consistencyLevel.name().toLowerCase()))
				.withHeader("Content-Type", containing("text/plain"))
				.withRequestBody(containing("taskmanager_" + metricName + ",host=" + METRIC_HOSTNAME + ",tm_id=" + METRIC_TM_ID + " count=42i")));
		} finally {
			metricRegistry.shutdown().get();
		}
	}

	private MetricRegistryImpl createMetricRegistry(String retentionPolicy, InfluxDB.ConsistencyLevel consistencyLevel) {
		MetricConfig metricConfig = new MetricConfig();
		metricConfig.setProperty(InfluxdbReporterOptions.HOST.key(), "localhost");
		metricConfig.setProperty(InfluxdbReporterOptions.PORT.key(), String.valueOf(wireMockRule.port()));
		metricConfig.setProperty(InfluxdbReporterOptions.DB.key(), TEST_INFLUXDB_DB);
		metricConfig.setProperty(InfluxdbReporterOptions.RETENTION_POLICY.key(), retentionPolicy);
		metricConfig.setProperty(InfluxdbReporterOptions.CONSISTENCY.key(), consistencyLevel.name());

		return new MetricRegistryImpl(
			MetricRegistryConfiguration.defaultMetricRegistryConfiguration(),
			Collections.singletonList(ReporterSetup.forReporter("test", metricConfig, new InfluxdbReporter())));
	}

	private static Counter registerTestMetric(String metricName, MetricRegistry metricRegistry) {
		TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(metricRegistry, METRIC_HOSTNAME, METRIC_TM_ID);
		return metricGroup.counter(metricName);
	}
}
