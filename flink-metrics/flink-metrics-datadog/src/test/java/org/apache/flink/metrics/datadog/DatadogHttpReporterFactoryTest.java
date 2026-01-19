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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DatadogHttpReporterFactory}. */
class DatadogHttpReporterFactoryTest {

    @Test
    void testMetricReporterSetupViaSPI() {
        MetricReporterTestUtils.testMetricReporterSetupViaSPI(DatadogHttpReporterFactory.class);
    }

    @Test
    void testApiKeyFromConfigurationProperty() throws Exception {
        final String apiKey = "test-api-key-from-config";
        final String resolvedApiKey = getApiKeyViaReflection(createConfig("apikey", apiKey));
        assertThat(resolvedApiKey).isEqualTo(apiKey);
    }

    @Test
    void testEmptyConfigurationPropertyReturnsNull() throws Exception {
        final String resolvedApiKey = getApiKeyViaReflection(createConfig("apikey", ""));
        // When config is empty, it falls back to System.getenv which returns null if not set
        assertThat(resolvedApiKey).isNull();
    }

    @Test
    void testNoConfigurationPropertyReturnsNull() throws Exception {
        final String resolvedApiKey = getApiKeyViaReflection(createConfig());
        // When no config is provided, it falls back to System.getenv which returns null if not set
        assertThat(resolvedApiKey).isNull();
    }

    private Properties createConfig(String key, String value) {
        Properties config = new Properties();
        config.setProperty(key, value);
        return config;
    }

    private Properties createConfig() {
        return new Properties();
    }

    private String getApiKeyViaReflection(Properties config) throws Exception {
        DatadogHttpReporterFactory factory = new DatadogHttpReporterFactory();
        // Use reflection to call the private getApiKey method
        java.lang.reflect.Method getApiKeyMethod =
                DatadogHttpReporterFactory.class.getDeclaredMethod("getApiKey", Properties.class);
        getApiKeyMethod.setAccessible(true);
        return (String) getApiKeyMethod.invoke(factory, config);
    }
}
