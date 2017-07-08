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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingHistogram;
import org.apache.flink.util.TestLogger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static org.apache.flink.metrics.prometheus.PrometheusReporter.ARG_PORT;
import static org.apache.flink.runtime.metrics.scope.ScopeFormat.SCOPE_SEPARATOR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link PrometheusReporter}.
 */
public class PrometheusReporterTest extends TestLogger {
	private static final int NON_DEFAULT_PORT = 9429;

	private static final String HOST_NAME = "hostname";
	private static final String TASK_MANAGER = "tm";

	private static final String HELP_PREFIX = "# HELP ";
	private static final String TYPE_PREFIX = "# TYPE ";
	private static final String DIMENSIONS = "host=\"" + HOST_NAME + "\",tm_id=\"" + TASK_MANAGER + "\"";
	private static final String DEFAULT_LABELS = "{" + DIMENSIONS + ",}";
	private static final String SCOPE_PREFIX = "flink_taskmanager_";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final MetricRegistry registry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(createConfigWithOneReporter()));
	private final MetricReporter reporter = registry.getReporters().get(0);

	@Test
	public void counterIsReportedAsPrometheusGauge() throws UnirestException {
		//Prometheus counters may not decrease
		Counter testCounter = new SimpleCounter();
		testCounter.inc(7);

		String counterName = "testCounter";
		String gaugeName = SCOPE_PREFIX + counterName;

		assertThat(addMetricAndPollResponse(testCounter, counterName),
			equalTo(HELP_PREFIX + gaugeName + " " + getFullMetricName(counterName) + "\n" +
				TYPE_PREFIX + gaugeName + " gauge" + "\n" +
				gaugeName + DEFAULT_LABELS + " 7.0" + "\n"));
	}

	@Test
	public void gaugeIsReportedAsPrometheusGauge() throws UnirestException {
		Gauge<Integer> testGauge = new Gauge<Integer>() {
			@Override
			public Integer getValue() {
				return 1;
			}
		};

		String gaugeName = "testGauge";
		String prometheusGaugeName = SCOPE_PREFIX + gaugeName;

		assertThat(addMetricAndPollResponse(testGauge, gaugeName),
			equalTo(HELP_PREFIX + prometheusGaugeName + " " + getFullMetricName(gaugeName) + "\n" +
				TYPE_PREFIX + prometheusGaugeName + " gauge" + "\n" +
				prometheusGaugeName + DEFAULT_LABELS + " 1.0" + "\n"));
	}

	@Test
	public void histogramIsReportedAsPrometheusSummary() throws UnirestException {
		Histogram testHistogram = new TestingHistogram();

		String histogramName = "testHistogram";
		String summaryName = SCOPE_PREFIX + histogramName;

		String response = addMetricAndPollResponse(testHistogram, histogramName);
		assertThat(response, containsString(HELP_PREFIX + summaryName + " " + getFullMetricName(histogramName) + "\n" +
			TYPE_PREFIX + summaryName + " summary" + "\n" +
			summaryName + "_count" + DEFAULT_LABELS + " 1.0" + "\n"));
		for (String quantile : Arrays.asList("0.5", "0.75", "0.95", "0.98", "0.99", "0.999")) {
			assertThat(response, containsString(
				summaryName + "{" + DIMENSIONS + ",quantile=\"" + quantile + "\",} " + quantile + "\n"));
		}
	}

	@Test
	public void meterRateIsReportedAsPrometheusGauge() throws UnirestException {
		Meter testMeter = new TestMeter();

		String meterName = "testMeter";
		String counterName = SCOPE_PREFIX + meterName;

		assertThat(addMetricAndPollResponse(testMeter, meterName),
			equalTo(HELP_PREFIX + counterName + " " + getFullMetricName(meterName) + "\n" +
				TYPE_PREFIX + counterName + " gauge" + "\n" +
				counterName + DEFAULT_LABELS + " 5.0" + "\n"));
	}

	@Test
	public void endpointIsUnavailableAfterReporterIsClosed() throws UnirestException {
		reporter.close();
		thrown.expect(UnirestException.class);
		pollMetrics();
	}

	@Test
	public void invalidCharactersAreReplacedWithUnderscore() {
		assertThat(PrometheusReporter.replaceInvalidChars(""), equalTo(""));
		assertThat(PrometheusReporter.replaceInvalidChars("abc"), equalTo("abc"));
		assertThat(PrometheusReporter.replaceInvalidChars("abc\""), equalTo("abc_"));
		assertThat(PrometheusReporter.replaceInvalidChars("\"abc"), equalTo("_abc"));
		assertThat(PrometheusReporter.replaceInvalidChars("\"abc\""), equalTo("_abc_"));
		assertThat(PrometheusReporter.replaceInvalidChars("\"a\"b\"c\""), equalTo("_a_b_c_"));
		assertThat(PrometheusReporter.replaceInvalidChars("\"\"\"\""), equalTo("____"));
		assertThat(PrometheusReporter.replaceInvalidChars("    "), equalTo("____"));
		assertThat(PrometheusReporter.replaceInvalidChars("\"ab ;(c)'"), equalTo("_ab___c__"));
		assertThat(PrometheusReporter.replaceInvalidChars("a b c"), equalTo("a_b_c"));
		assertThat(PrometheusReporter.replaceInvalidChars("a b c "), equalTo("a_b_c_"));
		assertThat(PrometheusReporter.replaceInvalidChars("a;b'c*"), equalTo("a_b_c_"));
		assertThat(PrometheusReporter.replaceInvalidChars("a,=;:?'b,=;:?'c"), equalTo("a___:__b___:__c"));
	}

	private String addMetricAndPollResponse(Metric metric, String metricName) throws UnirestException {
		reporter.notifyOfAddedMetric(metric, metricName, new FrontMetricGroup<>(0, new TaskManagerMetricGroup(registry, HOST_NAME, TASK_MANAGER)));
		return pollMetrics().getBody();
	}

	private static HttpResponse<String> pollMetrics() throws UnirestException {
		return Unirest.get("http://localhost:" + NON_DEFAULT_PORT + "/metrics").asString();
	}

	private static String getFullMetricName(String metricName) {
		return HOST_NAME + SCOPE_SEPARATOR + "taskmanager" + SCOPE_SEPARATOR + TASK_MANAGER + SCOPE_SEPARATOR + metricName;
	}

	private static Configuration createConfigWithOneReporter() {
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.REPORTERS_LIST, "test1");
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." +
			ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, PrometheusReporter.class.getName());
		cfg.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "test1." + ARG_PORT, "" + NON_DEFAULT_PORT);
		return cfg;
	}

	@After
	public void closeReporterAndShutdownRegistry() {
		reporter.close();
		registry.shutdown();
	}
}
