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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Rule;
import org.junit.Test;

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
public class InfluxdbReporterTest {
	private static final String TEST_INFLUXDB_DB = "test-42";
	private static final String METRIC_HOSTNAME = "task-mgr-1";
	private static final String METRIC_TM_ID = "tm-id-123";

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort().notifier(new ConsoleNotifier(false)));

	@Test
	public void test() throws Exception {
		String configPrefix = ConfigConstants.METRICS_REPORTER_PREFIX + "test.";
		Configuration configuration = new Configuration();
		configuration.setString(
			configPrefix + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
			InfluxdbReporter.class.getTypeName());
		configuration.setString(configPrefix + "host", "localhost");
		configuration.setString(configPrefix + "port", String.valueOf(wireMockRule.port()));
		configuration.setString(configPrefix + "db", TEST_INFLUXDB_DB);

		MetricRegistryImpl metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(configuration));
		MetricReporter reporter = metricRegistry.getReporters().get(0);
		assertTrue(reporter instanceof InfluxdbReporter);
		InfluxdbReporter influxdbReporter = (InfluxdbReporter) reporter;

		String metricName = "TestCounter";
		Counter counter = registerTestMetric(metricName, metricRegistry);
		MeasurementInfo measurementInfo = influxdbReporter.counters.get(counter);
		assertNotNull("test metric must be registered in the reporter", measurementInfo);
		String fullMetricName = "taskmanager_" + metricName;
		assertEquals(fullMetricName, measurementInfo.getName());
		assertThat(measurementInfo.getTags(), hasEntry("host", METRIC_HOSTNAME));
		assertThat(measurementInfo.getTags(), hasEntry("tm_id", METRIC_TM_ID));

		stubFor(post(urlPathEqualTo("/write"))
			.willReturn(aResponse()
				.withStatus(200)));

		counter.inc(42);
		influxdbReporter.report();

		verify(postRequestedFor(urlPathEqualTo("/write"))
			.withQueryParam("db", equalTo(TEST_INFLUXDB_DB))
			.withHeader("Content-Type", containing("text/plain"))
			.withRequestBody(containing(fullMetricName + ",host=" + METRIC_HOSTNAME + ",tm_id=" + METRIC_TM_ID + " count=42i")));

		metricRegistry.shutdown().get();
	}

	private static Counter registerTestMetric(String metricName, MetricRegistry metricRegistry) {
		TaskManagerMetricGroup metricGroup = new TaskManagerMetricGroup(metricRegistry, METRIC_HOSTNAME, METRIC_TM_ID);
		SimpleCounter counter = new SimpleCounter();
		metricGroup.counter(metricName, counter);
		return counter;
	}
}
