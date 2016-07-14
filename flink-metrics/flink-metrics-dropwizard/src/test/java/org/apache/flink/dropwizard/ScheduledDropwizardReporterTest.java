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

package org.apache.flink.dropwizard;

import com.codahale.metrics.ScheduledReporter;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricRegistry;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.groups.TaskMetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScheduledDropwizardReporterTest {

	@Test
	public void testInvalidCharacterReplacement() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		ScheduledDropwizardReporter reporter = new ScheduledDropwizardReporter() {
			@Override
			public ScheduledReporter getReporter(Configuration config) {
				return null;
			}
		};

		assertEquals("abc", reporter.filterCharacters("abc"));
		assertEquals("a--b-c-", reporter.filterCharacters("a..b.c."));
		assertEquals("ab-c", reporter.filterCharacters("a\"b.c"));
	}

	/**
	 * Tests that the registered metrics' names don't contain invalid characters.
	 */
	@Test
	public void testAddingMetrics() throws NoSuchFieldException, IllegalAccessException {
		Configuration configuration = new Configuration();
		String taskName = "test\"Ta\"..sk";
		String jobName = "testJ\"ob:-!ax..?";
		String hostname = "loc<>al\"::host\".:";
		String taskManagerId = "tas:kMana::ger";
		String counterName = "testCounter";

		configuration.setString(ConfigConstants.METRICS_REPORTER_CLASS, "org.apache.flink.dropwizard.ScheduledDropwizardReporterTest$TestingScheduledDropwizardReporter");
		configuration.setString(ConfigConstants.METRICS_SCOPE_NAMING_TASK, "<host>.<tm_id>.<job_name>");
		configuration.setString(ConfigConstants.METRICS_SCOPE_DELIMITER, "_");

		MetricRegistry metricRegistry = new MetricRegistry(configuration);

		char delimiter = metricRegistry.getDelimiter();

		TaskManagerMetricGroup tmMetricGroup = new TaskManagerMetricGroup(metricRegistry, hostname, taskManagerId);
		TaskManagerJobMetricGroup tmJobMetricGroup = new TaskManagerJobMetricGroup(metricRegistry, tmMetricGroup, new JobID(), jobName);
		TaskMetricGroup taskMetricGroup = new TaskMetricGroup(metricRegistry, tmJobMetricGroup, new AbstractID(), new AbstractID(), taskName, 0, 0);

		SimpleCounter myCounter = new SimpleCounter();

		taskMetricGroup.counter(counterName, myCounter);

		Field reporterField = MetricRegistry.class.getDeclaredField("reporter");
		reporterField.setAccessible(true);

		MetricReporter metricReporter = (MetricReporter) reporterField.get(metricRegistry);

		assertTrue("Reporter should be of type ScheduledDropwizardReporter", metricReporter instanceof ScheduledDropwizardReporter);

		TestingScheduledDropwizardReporter reporter = (TestingScheduledDropwizardReporter) metricReporter;

		Map<Counter, String> counters = reporter.getCounters();

		assertTrue(counters.containsKey(myCounter));

		String expectedCounterName = reporter.filterCharacters(hostname)
			+ delimiter
			+ reporter.filterCharacters(taskManagerId)
			+ delimiter
			+ reporter.filterCharacters(jobName)
			+ delimiter
			+ reporter.filterCharacters(counterName);

		assertEquals(expectedCounterName, counters.get(myCounter));

		metricRegistry.shutdown();
	}

	public static class TestingScheduledDropwizardReporter extends ScheduledDropwizardReporter {

		@Override
		public ScheduledReporter getReporter(Configuration config) {
			return null;
		}

		@Override
		public void close() {
			// don't do anything
		}
	}
}
