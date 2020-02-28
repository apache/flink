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

package org.apache.flink.metrics.slf4j;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.testutils.logging.TestLoggerResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link Slf4jReporter}.
 */
public class Slf4jReporterTest extends TestLogger {

	private static final String HOST_NAME = "localhost";
	private static final String TASK_MANAGER_ID = "tm01";
	private static final String JOB_NAME = "jn01";
	private static final String TASK_NAME = "tn01";
	private static MetricRegistryImpl registry;
	private static char delimiter;
	private static TaskMetricGroup taskMetricGroup;
	private static Slf4jReporter reporter;

	@Rule
	public final TestLoggerResource testLoggerResource = new TestLoggerResource(Slf4jReporter.class, Level.INFO);

	@BeforeClass
	public static void setUp() {
		Configuration configuration = new Configuration();
		configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");

		registry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			Collections.singletonList(ReporterSetup.forReporter("slf4j", new Slf4jReporter())));
		delimiter = registry.getDelimiter();

		taskMetricGroup = new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER_ID)
			.addTaskForJob(new JobID(), JOB_NAME, new JobVertexID(), new ExecutionAttemptID(), TASK_NAME, 0, 0);
		reporter = (Slf4jReporter) registry.getReporters().get(0);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		registry.shutdown().get();
	}

	@Test
	public void testAddCounter() throws Exception {
		String counterName = "simpleCounter";

		SimpleCounter counter = new SimpleCounter();
		taskMetricGroup.counter(counterName, counter);

		assertTrue(reporter.getCounters().containsKey(counter));

		String expectedCounterReport = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(counterName) + ": 0";

		reporter.report();
		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(expectedCounterReport)));
	}

	@Test
	public void testAddGauge() throws Exception {
		String gaugeName = "gauge";

		taskMetricGroup.gauge(gaugeName, null);
		assertTrue(reporter.getGauges().isEmpty());

		Gauge<Long> gauge = () -> null;
		taskMetricGroup.gauge(gaugeName, gauge);
		assertTrue(reporter.getGauges().containsKey(gauge));

		String expectedGaugeReport = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(gaugeName) + ": null";

		reporter.report();
		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(expectedGaugeReport)));
	}

	@Test
	public void testAddMeter() throws Exception {
		String meterName = "meter";

		Meter meter = taskMetricGroup.meter(meterName, new MeterView(5));
		assertTrue(reporter.getMeters().containsKey(meter));

		String expectedMeterReport = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(meterName) + ": 0.0";

		reporter.report();
		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(expectedMeterReport)));
	}

	@Test
	public void testAddHistogram() throws Exception {
		String histogramName = "histogram";

		Histogram histogram = taskMetricGroup.histogram(histogramName, new TestHistogram());
		assertTrue(reporter.getHistograms().containsKey(histogram));

		String expectedHistogramName = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(histogramName);

		reporter.report();
		assertThat(
			testLoggerResource.getMessages(),
			hasItem(containsString(expectedHistogramName)));
	}

	@Test
	public void testFilterCharacters() throws Exception {
		Slf4jReporter reporter = new Slf4jReporter();

		assertThat(reporter.filterCharacters(""), equalTo(""));
		assertThat(reporter.filterCharacters("abc"), equalTo("abc"));
		assertThat(reporter.filterCharacters("a:b$%^::"), equalTo("a:b$%^::"));
	}
}
