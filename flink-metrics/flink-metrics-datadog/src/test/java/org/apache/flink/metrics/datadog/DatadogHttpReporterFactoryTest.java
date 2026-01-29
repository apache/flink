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

    @Test
    void testApiKeyFromEnvironmentVariable() throws Exception {
        final String envKey = "env-api-key";
        setEnv("DD_API_KEY", envKey);
        try {
            final String resolvedApiKey = getApiKeyViaReflection(createConfig());
            assertThat(resolvedApiKey).isEqualTo(envKey);
        } finally {
            setEnv("DD_API_KEY", null);
        }
    }

    @Test
    void testEnvironmentVariableTakesPrecedenceOverConfiguration() throws Exception {
        final String envKey = "env-api-key";
        final String configKey = "config-api-key";
        setEnv("DD_API_KEY", envKey);
        try {
            final String resolvedApiKey = getApiKeyViaReflection(createConfig("apikey", configKey));
            assertThat(resolvedApiKey).isEqualTo(envKey);
        } finally {
            setEnv("DD_API_KEY", null);
        }
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

    @SuppressWarnings("unchecked")
    private void setEnv(String key, String value) throws Exception {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            java.lang.reflect.Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            if (value == null) {
                writableEnv.remove(key);
            } else {
                writableEnv.put(key, value);
            }
        } catch (NoSuchFieldException e) {
            // Fallback for other JVM implementations
            try {
                Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
                java.lang.reflect.Field theEnvironmentField = pe.getDeclaredField("theEnvironment");
                theEnvironmentField.setAccessible(true);
                Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
                if (value == null) {
                    env.remove(key);
                } else {
                    env.put(key, value);
                }
                java.lang.reflect.Field theCaseInsensitiveEnvironmentField = pe.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironmentField.setAccessible(true);
                Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
                if (value == null) {
                    cienv.remove(key);
                } else {
                    cienv.put(key, value);
                }
            } catch (ClassNotFoundException | NoSuchFieldException ex) {
                // ignore; best-effort environment mutation for tests
            }
        }
    }
}
