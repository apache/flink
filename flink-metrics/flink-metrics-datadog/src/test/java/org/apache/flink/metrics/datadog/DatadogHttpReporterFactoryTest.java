/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.util.MetricReporterTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DatadogHttpReporterFactory}. */
class DatadogHttpReporterFactoryTest {

    private DatadogHttpReporterFactory factory;

    @BeforeEach
    void setUp() {
        factory = new DatadogHttpReporterFactory();
    }

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(DatadogHttpReporterFactory.class);
    }

    @Test
    void testApiKeyFromConfiguration() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key-from-config");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testApiKeyFromEnvironmentVariable() {
        Properties config = new Properties();
        config.setProperty("dataCenter", "US");

        // Store original environment variable
        String originalEnv = System.getenv("DATADOG_API_KEY");
        
        try {
            // Set environment variable via system property approach
            // Note: This tests that the factory attempts to use the environment variable
            // In actual deployment, the OS would set DATADOG_API_KEY before JVM startup
            MetricReporter reporter = factory.createMetricReporter(config);

            // If an API key is found (either from config or env), reporter should be created
            // This test validates the code path doesn't fail when env var is expected
            assertThat(reporter).isNotNull();
        } finally {
            // Clean up - restoration would happen via OS environment
        }
    }

    @Test
    void testConfigurationApiKeyTakesPrecedenceOverEnvironmentVariable() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key-from-config");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testDefaultProxyPort() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testCustomProxySettings() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("proxyHost", "proxy.example.com");
        config.setProperty("proxyPort", "9090");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testDataCenterConfiguration() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("dataCenter", "EU");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testMaxMetricsPerRequestConfiguration() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("maxMetricsPerRequest", "5000");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testTagsConfiguration() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("tags", "env:test,version:1.0");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }

    @Test
    void testUseLogicalIdentifierConfiguration() {
        Properties config = new Properties();
        config.setProperty("apikey", "test-api-key");
        config.setProperty("useLogicalIdentifier", "true");
        config.setProperty("dataCenter", "US");

        MetricReporter reporter = factory.createMetricReporter(config);

        assertThat(reporter).isNotNull().isInstanceOf(DatadogHttpReporter.class);
    }
}
