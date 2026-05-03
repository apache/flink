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

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Map;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST_URL;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PASSWORD;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PrometheusPushGatewayReporter}. */
class PrometheusPushGatewayReporterTest {

    @RegisterExtension
    private final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(PrometheusPushGatewayReporterFactory.class, Level.WARN);

    @Test
    void testParseGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=v1;k2=v2");
        assertThat(groupingKey).containsEntry("k1", "v1");
        assertThat(groupingKey).containsEntry("k2", "v2");
    }

    @Test
    void testParseIncompleteGroupingKey() {
        Map<String, String> groupingKey =
                PrometheusPushGatewayReporterFactory.parseGroupingKey("k1=");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("=v1");
        assertThat(groupingKey).isEmpty();

        groupingKey = PrometheusPushGatewayReporterFactory.parseGroupingKey("k1");
        assertThat(groupingKey).isEmpty();
    }

    @Test
    void testConnectToPushGatewayUsingHostUrl() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "https://localhost:18080");
        String gatewayBaseURL = factory.createMetricReporter(metricConfig).hostUrl.toString();
        assertThat(gatewayBaseURL).isEqualTo("https://localhost:18080");
    }

    @Test
    void testConnectToPushGatewayThrowsExceptionWithoutHostInformation() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        assertThatThrownBy(() -> factory.createMetricReporter(metricConfig))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBasicAuthNotEnabledWithoutCredentials() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "http://localhost:9091");
        // No authentication configured - should create reporter successfully without Basic Auth
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        assertThat(reporter).isNotNull();
        assertThat(reporter.hostUrl.toString()).isEqualTo("http://localhost:9091");
        assertThat(reporter.basicAuthEnabled).isFalse();
    }

    @Test
    void testBasicAuthNotEnabledWithOnlyUsername() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "http://localhost:9091");
        metricConfig.setProperty(USERNAME.key(), "flink-user");
        // Only username configured - should create reporter successfully without Basic Auth
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        assertThat(reporter).isNotNull();
        assertThat(reporter.basicAuthEnabled).isFalse();
        // Verify warning log is emitted
        assertThat(loggerExtension.getMessages())
                .anyMatch(
                        msg ->
                                msg.contains("Both username and password must be configured")
                                        && msg.contains("Currently only username is configured"));
    }

    @Test
    void testBasicAuthNotEnabledWithOnlyPassword() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "http://localhost:9091");
        metricConfig.setProperty(PASSWORD.key(), "flink-password");
        // Only password configured - should create reporter successfully without Basic Auth
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        assertThat(reporter).isNotNull();
        assertThat(reporter.basicAuthEnabled).isFalse();
        // Verify warning log is emitted
        assertThat(loggerExtension.getMessages())
                .anyMatch(
                        msg ->
                                msg.contains("Both username and password must be configured")
                                        && msg.contains("Currently only password is configured"));
    }

    @Test
    void testBasicAuthEnabledWithBothCredentials() {
        PrometheusPushGatewayReporterFactory factory = new PrometheusPushGatewayReporterFactory();
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(HOST_URL.key(), "http://localhost:9091");
        metricConfig.setProperty(USERNAME.key(), "flink-user");
        metricConfig.setProperty(PASSWORD.key(), "flink-password");
        // Both username and password configured - Basic Auth should be enabled
        PrometheusPushGatewayReporter reporter = factory.createMetricReporter(metricConfig);
        assertThat(reporter).isNotNull();
        assertThat(reporter.hostUrl.toString()).isEqualTo("http://localhost:9091");
        assertThat(reporter.basicAuthEnabled).isTrue();
    }
}
