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

package org.apache.flink.connector.kinesis.table.util;

import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Unit tests for {@link KinesisAsyncClientOptionsUtils}. */
public class KinesisAsyncClientOptionsUtilsTest extends TestLogger {

    @Test
    public void testGoodKinesisClientOptionsMapping() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultClientOptions);
        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                kinesisAsyncClientOptionsUtils.getProcessedResolvedOptions();
        Assertions.assertThat(actualConfigurations).isEqualTo(expectedConfigurations);
    }

    @Test
    public void testGoodKinesisClientOptionsSelectionAndMapping() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.not.http-client.some.option", "someValue");
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultClientOptions);
        Map<String, String> expectedConfigurations = getDefaultExpectedClientOptions();
        Map<String, String> actualConfigurations =
                kinesisAsyncClientOptionsUtils.getProcessedResolvedOptions();
        Assertions.assertThat(actualConfigurations).isEqualTo(expectedConfigurations);
    }

    @Test
    public void testGoodKinesisClientConfigurations() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultClientOptions);
        Properties expectedConfigurations = getDefaultExpectedClientConfigs();
        Properties actualConfigurations =
                kinesisAsyncClientOptionsUtils.getValidatedConfigurations();
        Assertions.assertThat(actualConfigurations).isEqualTo(expectedConfigurations);
    }

    @Test
    public void testBadKinesisClientMaxConcurrency() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.http-client.max-concurrency", "invalid-integer");
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultClientOptions);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(kinesisAsyncClientOptionsUtils::getValidatedConfigurations)
                .withMessageContaining(
                        "Invalid value given for HTTP client max concurrency. Must be positive integer.");
    }

    @Test
    public void testBadKinesisClientReadTimeout() {
        Map<String, String> defaultClientOptions = getDefaultClientOptions();
        defaultClientOptions.put("sink.http-client.read-timeout", "invalid-integer");
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultClientOptions);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(kinesisAsyncClientOptionsUtils::getValidatedConfigurations)
                .withMessageContaining(
                        "Invalid value given for HTTP read timeout. Must be positive integer.");
    }

    @Test
    public void testBadKinesisClientHttpVersion() {
        Map<String, String> defaultProperties = getDefaultClientOptions();
        defaultProperties.put("sink.http-client.protocol.version", "invalid-http-protocol");
        KinesisAsyncClientOptionsUtils kinesisAsyncClientOptionsUtils =
                new KinesisAsyncClientOptionsUtils(defaultProperties);
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(kinesisAsyncClientOptionsUtils::getValidatedConfigurations)
                .withMessageContaining(
                        "Invalid value given for HTTP protocol. Must be HTTP1_1 or HTTP2.");
    }

    private static Map<String, String> getDefaultClientOptions() {
        Map<String, String> defaultKinesisClientOptions = new HashMap<String, String>();
        defaultKinesisClientOptions.put("aws.region", "us-east-1");
        defaultKinesisClientOptions.put("sink.http-client.max-concurrency", "10000");
        defaultKinesisClientOptions.put("sink.http-client.read-timeout", "360000");
        defaultKinesisClientOptions.put("sink.http-client.protocol.version", "HTTP2");
        return defaultKinesisClientOptions;
    }

    private static Map<String, String> getDefaultExpectedClientOptions() {
        Map<String, String> defaultExpectedKinesisClientConfigurations =
                new HashMap<String, String>();
        defaultExpectedKinesisClientConfigurations.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, "10000");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS, "360000");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP2");
        return defaultExpectedKinesisClientConfigurations;
    }

    private static Properties getDefaultExpectedClientConfigs() {
        Properties defaultExpectedKinesisClientConfigurations = new Properties();
        defaultExpectedKinesisClientConfigurations.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_MAX_CONCURRENCY, "10000");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_CLIENT_READ_TIMEOUT_MILLIS, "360000");
        defaultExpectedKinesisClientConfigurations.put(
                AWSConfigConstants.HTTP_PROTOCOL_VERSION, "HTTP2");
        return defaultExpectedKinesisClientConfigurations;
    }
}
