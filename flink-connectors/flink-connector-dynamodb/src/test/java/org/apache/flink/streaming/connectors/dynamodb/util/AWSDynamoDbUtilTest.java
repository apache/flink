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

package org.apache.flink.streaming.connectors.dynamodb.util;

import org.apache.flink.connector.aws.util.TestUtil;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSDynamoDbConfigConstants;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;

import java.time.Duration;
import java.util.Properties;

/** Tests for {@link AWSDynamoDbUtil}. */
public class AWSDynamoDbUtilTest {

    @Test
    public void testHasRetryConfiguration() {
        Properties properties =
                TestUtil.properties(
                        AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY, "FULL_JITTER");

        Assertions.assertThat(AWSDynamoDbUtil.hasRetryConfiguration(properties))
                .as("Should have retry configuration")
                .isTrue();
    }

    @Test
    public void testConstructsFullJitterBackoffStrategy() {
        Properties properties = new Properties();
        properties.put(AWSDynamoDbConfigConstants.BACKOFF_STRATEGY, "FULL_JITTER");
        properties.put(AWSDynamoDbConfigConstants.FULL_JITTER_BASE_DELAY_MS, "100");
        properties.put(AWSDynamoDbConfigConstants.FULL_JITTER_MAX_BACKOFF_TIME_MS, "10");

        BackoffStrategy backoffStrategy =
                AWSDynamoDbUtil.getBackoffStrategy(
                        properties, AWSDynamoDbConfigConstants.BACKOFF_STRATEGY);

        Assertions.assertThat(backoffStrategy).isInstanceOf(FullJitterBackoffStrategy.class);
        Assertions.assertThat(((FullJitterBackoffStrategy) backoffStrategy).toBuilder().baseDelay())
                .isEqualTo(Duration.ofMillis(100));
        Assertions.assertThat(
                        ((FullJitterBackoffStrategy) backoffStrategy).toBuilder().maxBackoffTime())
                .isEqualTo(Duration.ofMillis(10));
    }

    @Test
    public void testConstructsEqualJitterBackoffStrategy() {
        Properties properties = new Properties();
        properties.put(AWSDynamoDbConfigConstants.BACKOFF_STRATEGY, "EQUAL_JITTER");
        properties.put(AWSDynamoDbConfigConstants.EQUAL_JITTER_BASE_DELAY_MS, "10");
        properties.put(AWSDynamoDbConfigConstants.EQUAL_JITTER_MAX_BACKOFF_TIME_MS, "100");

        BackoffStrategy backoffStrategy =
                AWSDynamoDbUtil.getBackoffStrategy(
                        properties, AWSDynamoDbConfigConstants.BACKOFF_STRATEGY);

        Assertions.assertThat(backoffStrategy).isInstanceOf(EqualJitterBackoffStrategy.class);
        Assertions.assertThat(
                        ((EqualJitterBackoffStrategy) backoffStrategy).toBuilder().baseDelay())
                .isEqualTo(Duration.ofMillis(10));
        Assertions.assertThat(
                        ((EqualJitterBackoffStrategy) backoffStrategy).toBuilder().maxBackoffTime())
                .isEqualTo(Duration.ofMillis(100));
    }

    @Test
    public void testConstructsFixedDelayBackoffStrategy() {
        Properties properties = new Properties();
        properties.put(AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY, "FIXED_DELAY");
        properties.put(AWSDynamoDbConfigConstants.FIXED_DELAY_BACKOFF_MS, "10");

        BackoffStrategy backoffStrategy =
                AWSDynamoDbUtil.getBackoffStrategy(
                        properties, AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY);

        Assertions.assertThat(backoffStrategy).isInstanceOf(FixedDelayBackoffStrategy.class);
        Assertions.assertThat(backoffStrategy)
                .isEqualTo(FixedDelayBackoffStrategy.create(Duration.ofMillis(10)));
    }

    @Test
    public void testConstructsRetryPolicy() {
        Properties properties = new Properties();

        properties.put(AWSDynamoDbConfigConstants.BACKOFF_STRATEGY, "FIXED_DELAY");
        properties.put(AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY, "FIXED_DELAY");

        int expectedNumRetries = 100;
        properties.put(
                AWSDynamoDbConfigConstants.NUMBER_RETRIES, String.valueOf(expectedNumRetries));
        int expectedDurationMs = 10;
        BackoffStrategy expectedStrategy =
                FixedDelayBackoffStrategy.create(Duration.ofMillis(expectedDurationMs));
        properties.put(
                AWSDynamoDbConfigConstants.FIXED_DELAY_BACKOFF_MS,
                String.valueOf(expectedDurationMs));

        RetryPolicy policy = AWSDynamoDbUtil.getRetryPolicy(properties);

        Assertions.assertThat(policy.toBuilder().numRetries()).isEqualTo(expectedNumRetries);
        Assertions.assertThat(policy.toBuilder().backoffStrategy()).isEqualTo(expectedStrategy);
        Assertions.assertThat(policy.toBuilder().throttlingBackoffStrategy())
                .isEqualTo(expectedStrategy);
    }

    @Test
    public void testConstructsOverrideConfigurationWithDefaults() {
        Properties properties = new Properties();

        ClientOverrideConfiguration configuration =
                AWSDynamoDbUtil.getOverrideConfiguration(properties);

        Assertions.assertThat(
                        getAdvancedOption(configuration, SdkAdvancedClientOption.USER_AGENT_PREFIX))
                .isEqualTo(AWSDynamoDbUtil.getFlinkUserAgentPrefix(properties));

        Assertions.assertThat(configuration.retryPolicy().isPresent()).isTrue();
        Assertions.assertThat(configuration.retryPolicy().get())
                .isEqualTo(RetryPolicy.defaultRetryPolicy());
        Assertions.assertThat(
                        getAdvancedOption(configuration, SdkAdvancedClientOption.USER_AGENT_SUFFIX))
                .isNull();
        Assertions.assertThat(configuration.apiCallTimeout().isPresent()).isFalse();
        Assertions.assertThat(configuration.apiCallAttemptTimeout().isPresent()).isFalse();
    }

    private String getAdvancedOption(
            ClientOverrideConfiguration configuration,
            SdkAdvancedClientOption<String> userAgentPrefix) {
        return configuration.toBuilder().advancedOptions().toBuilder().get(userAgentPrefix);
    }
}
