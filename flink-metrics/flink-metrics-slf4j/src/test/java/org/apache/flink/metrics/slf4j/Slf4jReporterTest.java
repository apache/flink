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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
	private static MetricRegistry registry;
	private static char delimiter;
	private static TaskMetricGroup taskMetricGroup;
	private static Slf4jReporter reporter;

	@BeforeClass
	public static void setUp() {
		TestUtils.addTestAppenderForRootLogger();

		Configuration configuration = new Configuration();
		configuration.setString(MetricOptions.REPORTERS_LIST, "slf4j");
		configuration.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "slf4j." +
			ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, Slf4jReporter.class.getName());
		configuration.setString(MetricOptions.SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");

		registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(configuration));
		delimiter = registry.getDelimiter();

		TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER_ID);
		TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(registry, tmMetricGroup, new JobID(), JOB_NAME);
		taskMetricGroup = new TaskMetricGroup(registry, tmJobMetricGroup, new AbstractID(), new AbstractID(), TASK_NAME, 0, 0);
		reporter = (Slf4jReporter) registry.getReporters().get(0);
	}

	@AfterClass
	public static void tearDown() {
		registry.shutdown();
	}

	@Test
	public void testAddCounter() throws Exception {
		String myCounterName = "simpleCounter";

		SimpleCounter simpleCounter = new SimpleCounter();
		taskMetricGroup.counter(myCounterName, simpleCounter);

		assertTrue(reporter.getCounters().containsKey(simpleCounter));

		String expectedCounterName = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(myCounterName);

		reporter.report();
		TestUtils.checkForLogString(expectedCounterName);
	}

	@Test
	public void testAddGague() throws Exception {
		String myGagueName = "gague";

		taskMetricGroup.gauge(myGagueName, null);
		assertTrue(reporter.getGauges().isEmpty());

		taskMetricGroup.gauge(myGagueName, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return null;
			}
		});
		String expectedTheGaugeReport = reporter.filterCharacters(HOST_NAME) + delimiter
			+ reporter.filterCharacters(TASK_MANAGER_ID) + delimiter + reporter.filterCharacters(JOB_NAME) + delimiter
			+ reporter.filterCharacters(myGagueName) + ": value=null";

		reporter.report();
		TestUtils.checkForLogString(expectedTheGaugeReport);
	}

	@Test
	public void testFilterCharacters() throws Exception {
		Slf4jReporter reporter = new Slf4jReporter();

		assertThat(reporter.filterCharacters(""), equalTo(""));
		assertThat(reporter.filterCharacters("abc"), equalTo("abc"));
		assertThat(reporter.filterCharacters("a:b::"), equalTo("a-b--"));
	}
}
