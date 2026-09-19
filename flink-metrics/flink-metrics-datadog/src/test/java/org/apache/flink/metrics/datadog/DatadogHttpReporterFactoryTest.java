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

import org.apache.flink.metrics.util.MetricReporterTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DatadogHttpReporterFactory}. */
class DatadogHttpReporterFactoryTest {

    private static final String API_KEY_ENV_VAR = "DD_API_KEY";

    private final DatadogHttpReporterFactory factory = new DatadogHttpReporterFactory();

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(DatadogHttpReporterFactory.class);
    }

    @Test
    void testApiKeyFromConfigurationProperty() {
        final String apiKey = "test-api-key-from-config";
        assertThat(factory.getApiKey(configWithApiKey(apiKey), envOf(null))).isEqualTo(apiKey);
    }

    @Test
    void testEmptyConfigurationPropertyReturnsNull() {
        assertThat(factory.getApiKey(configWithApiKey(""), envOf(null))).isNull();
    }

    @Test
    void testNoConfigurationPropertyReturnsNull() {
        assertThat(factory.getApiKey(new Properties(), envOf(null))).isNull();
    }

    @Test
    void testApiKeyFromEnvironmentVariable() {
        final String envKey = "env-api-key";
        assertThat(factory.getApiKey(new Properties(), envOf(envKey))).isEqualTo(envKey);
    }

    @Test
    void testEnvironmentVariableTakesPrecedenceOverConfiguration() {
        final String envKey = "env-api-key";
        final String configKey = "config-api-key";
        assertThat(factory.getApiKey(configWithApiKey(configKey), envOf(envKey))).isEqualTo(envKey);
    }

    private static Properties configWithApiKey(String value) {
        Properties config = new Properties();
        config.setProperty("apikey", value);
        return config;
    }

    private static Function<String, String> envOf(String ddApiKeyValue) {
        return name -> API_KEY_ENV_VAR.equals(name) ? ddApiKeyValue : null;
    }
}
