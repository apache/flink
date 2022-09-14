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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestMetricGroup;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import org.influxdb.InfluxDB;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for {@link InfluxdbReporter}. */
class InfluxdbReporterTest {
    private static final String TEST_INFLUXDB_DB = "test-42";
    private static final String METRIC_HOSTNAME = "task-mgr-1";

    private static final Map<String, String> variables;
    private static final MetricGroup metricGroup;

    static {
        variables = new HashMap<>();
        variables.put("<host>", METRIC_HOSTNAME);

        metricGroup =
                TestMetricGroup.newBuilder()
                        .setLogicalScopeFunction((characterFilter, character) -> "taskmanager")
                        .setVariables(variables)
                        .build();
    }

    @RegisterExtension private final WireMockExtension wireMockRule = new WireMockExtension();

    @Test
    void testMetricRegistration() {
        InfluxdbReporter reporter =
                createReporter(
                        InfluxdbReporterOptions.RETENTION_POLICY.defaultValue(),
                        InfluxdbReporterOptions.CONSISTENCY.defaultValue());

        String metricName = "TestCounter";
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, metricName, metricGroup);

        MeasurementInfo measurementInfo = reporter.counters.get(counter);
        assertThat(measurementInfo).isNotNull();
        assertThat(measurementInfo.getName()).isEqualTo("taskmanager_" + metricName);
        assertThat(measurementInfo.getTags()).containsEntry("host", METRIC_HOSTNAME);
    }

    @Test
    void testMetricReporting() {
        String retentionPolicy = "one_hour";
        InfluxDB.ConsistencyLevel consistencyLevel = InfluxDB.ConsistencyLevel.ANY;
        InfluxdbReporter reporter = createReporter(retentionPolicy, consistencyLevel);

        String metricName = "TestCounter";
        Counter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, metricName, metricGroup);
        counter.inc(42);

        stubFor(post(urlPathEqualTo("/write")).willReturn(aResponse().withStatus(200)));

        reporter.report();

        verify(
                postRequestedFor(urlPathEqualTo("/write"))
                        .withQueryParam("db", equalTo(TEST_INFLUXDB_DB))
                        .withQueryParam("rp", equalTo(retentionPolicy))
                        .withQueryParam(
                                "consistency", equalTo(consistencyLevel.name().toLowerCase()))
                        .withHeader("Content-Type", containing("text/plain"))
                        .withRequestBody(
                                containing(
                                        "taskmanager_"
                                                + metricName
                                                + ",host="
                                                + METRIC_HOSTNAME
                                                + " count=42i")));
    }

    private InfluxdbReporter createReporter(
            String retentionPolicy, InfluxDB.ConsistencyLevel consistencyLevel) {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(InfluxdbReporterOptions.HOST.key(), "localhost");
        metricConfig.setProperty(
                InfluxdbReporterOptions.PORT.key(), String.valueOf(wireMockRule.getPort()));
        metricConfig.setProperty(InfluxdbReporterOptions.DB.key(), TEST_INFLUXDB_DB);
        metricConfig.setProperty(InfluxdbReporterOptions.RETENTION_POLICY.key(), retentionPolicy);
        metricConfig.setProperty(
                InfluxdbReporterOptions.CONSISTENCY.key(), consistencyLevel.name());

        final InfluxdbReporter reporter = new InfluxdbReporter();
        reporter.open(metricConfig);
        return reporter;
    }
}
