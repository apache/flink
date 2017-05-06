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

package org.apache.flink.metrics.prometheus;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingHistogram;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.apache.flink.dropwizard.ScheduledDropwizardReporter.ARG_PORT;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class PrometheusReporterTest extends TestLogger {
	private static final int NON_DEFAULT_PORT = 9429;

	private static final String HOST_NAME = "host";
	private static final String TASK_MANAGER = "tm";

	private static final String HELP_PREFIX = "# HELP ";
	private static final String TYPE_PREFIX = "# TYPE ";

	private static final String SEPARATOR = "_";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private MetricRegistry registry = prepareMetricRegistry();

	private final MetricReporter reporter = registry.getReporters().get(0);

	@Test
	public void counterIsReportedAsPrometheusGauge() throws UnirestException {
		//Prometheus counters may not decrease
		Counter testCounter = new SimpleCounter();
		testCounter.inc(7);

		final String metricName = "counter";

		String classifier = getMetricClassifier(metricName);
		assertThat(addMetricAndPollResponse(testCounter, metricName), allOf(
			containsString(HELP_PREFIX + classifier),
			containsString(TYPE_PREFIX + classifier + " gauge"),
			containsString(classifier + " 7.0")));
	}

	@Test
	public void gaugeIsReportedAsPrometheusGauge() throws UnirestException {
		Gauge<Integer> testGauge = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		};

		String metricName = "gauge";

		String classifier = getMetricClassifier(metricName);
		assertThat(addMetricAndPollResponse(testGauge, metricName), allOf(
			containsString(HELP_PREFIX + classifier),
			containsString(TYPE_PREFIX + classifier + " gauge"),
			containsString(classifier + " 1.0")));
	}

	@Test
	public void histogramIsReportedAsPrometheusSummary() throws UnirestException {
		Histogram testHistogram = new TestingHistogram();

		String metricName = "histogram";

		String classifier = getMetricClassifier(metricName);
		String response = addMetricAndPollResponse(testHistogram, metricName);
		assertThat(response, allOf(
			containsString(HELP_PREFIX + classifier),
			containsString(TYPE_PREFIX + classifier + " summary"),
			containsString(classifier + "_count 1.0")));
		for (String quantile : Arrays.asList("0.5", "0.75", "0.95", "0.98", "0.99", "0.999")) {
			assertThat(response, containsString(classifier + "{quantile=\"" + quantile + "\",} " + quantile));
		}
	}

	@Test
	public void meterTotalIsReportedAsPrometheusCounter() throws UnirestException {
		Meter testMeter = new TestMeter();

		String metricName = "meter";

		String classifier = getMetricClassifier(metricName);
		assertThat(addMetricAndPollResponse(testMeter, metricName), allOf(
			containsString(HELP_PREFIX + classifier + "_total"),
			containsString(TYPE_PREFIX + classifier + "_total counter"),
			containsString(classifier + "_total 100.0")));
	}

	@Test
	public void endpointIsUnavailableAfterReporterIsClosed() throws UnirestException {
		reporter.close();
		thrown.expect(UnirestException.class);
		pollMetrics();
	}

	private String addMetricAndPollResponse(Metric metric, String metricName) throws UnirestException {
		reporter.notifyOfAddedMetric(metric, metricName, new FrontMetricGroup<>(0, new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER)));
		return pollMetrics().getBody();
	}

	private static HttpResponse<String> pollMetrics() throws UnirestException {
		return Unirest.get("http://localhost:" + NON_DEFAULT_PORT + "/metrics").asString();
	}

	private static String getMetricClassifier(String metricName) {
		return HOST_NAME + SEPARATOR + "taskmanager" + SEPARATOR + TASK_MANAGER + SEPARATOR + metricName;
	}

	private static MetricRegistry prepareMetricRegistry() {
		return new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(createConfigWithOneReporter()));
	}

	private static Configuration createConfigWithOneReporter() {
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.REPORTERS_LIST, "test1");
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, PrometheusReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ARG_PORT, "" + NON_DEFAULT_PORT);
		return cfg;
	}

	@After
	public void closeReporterAndShutdownRegistry() {
		reporter.close();
		registry.shutdown();
	}
}
