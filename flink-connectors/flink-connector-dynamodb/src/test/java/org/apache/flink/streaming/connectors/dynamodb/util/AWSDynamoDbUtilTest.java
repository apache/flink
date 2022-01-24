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

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;

import java.time.Duration;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/** Tests for {@link AWSDynamoDbUtil}. */
public class AWSDynamoDbUtilTest {

    @Test
    public void testHasRetryConfiguration() {
        Properties properties =
                TestUtil.properties(
                        AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY, "FULL_JITTER");

        assertTrue(
                "Should have retry configuration",
                AWSDynamoDbUtil.hasRetryConfiguration(properties));
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

        Assertions.assertInstanceOf(FullJitterBackoffStrategy.class, backoffStrategy);
        Assertions.assertEquals(
                Duration.ofMillis(100),
                ((FullJitterBackoffStrategy) backoffStrategy).toBuilder().baseDelay());
        Assertions.assertEquals(
                Duration.ofMillis(10),
                ((FullJitterBackoffStrategy) backoffStrategy).toBuilder().maxBackoffTime());
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

        Assertions.assertInstanceOf(EqualJitterBackoffStrategy.class, backoffStrategy);
        Assertions.assertEquals(
                Duration.ofMillis(10),
                ((EqualJitterBackoffStrategy) backoffStrategy).toBuilder().baseDelay());
        Assertions.assertEquals(
                Duration.ofMillis(100),
                ((EqualJitterBackoffStrategy) backoffStrategy).toBuilder().maxBackoffTime());
    }

    @Test
    public void testConstructsFixedDelayBackoffStrategy() {
        Properties properties = new Properties();
        properties.put(AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY, "FIXED_DELAY");
        properties.put(AWSDynamoDbConfigConstants.FIXED_DELAY_BACKOFF_MS, "10");

        BackoffStrategy backoffStrategy =
                AWSDynamoDbUtil.getBackoffStrategy(
                        properties, AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY);

        Assertions.assertInstanceOf(FixedDelayBackoffStrategy.class, backoffStrategy);
        Assertions.assertEquals(
                FixedDelayBackoffStrategy.create(Duration.ofMillis(10)), backoffStrategy);
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

        Assertions.assertEquals(expectedNumRetries, policy.toBuilder().numRetries());
        Assertions.assertEquals(expectedStrategy, policy.toBuilder().backoffStrategy());
        Assertions.assertEquals(expectedStrategy, policy.toBuilder().throttlingBackoffStrategy());
    }

    @Test
    public void testConstructsOverrideConfigurationWithDefaults() {
        Properties properties = new Properties();

        ClientOverrideConfiguration configuration =
                AWSDynamoDbUtil.getOverrideConfiguration(properties);

        Assertions.assertEquals(
                AWSDynamoDbUtil.getFlinkUserAgentPrefix(properties),
                getAdvancedOption(configuration, SdkAdvancedClientOption.USER_AGENT_PREFIX));

        Assertions.assertTrue(configuration.retryPolicy().isPresent());
        Assertions.assertEquals(
                RetryPolicy.defaultRetryPolicy(), configuration.retryPolicy().get());
        Assertions.assertNull(
                getAdvancedOption(configuration, SdkAdvancedClientOption.USER_AGENT_SUFFIX));
        Assertions.assertFalse(configuration.apiCallTimeout().isPresent());
        Assertions.assertFalse(configuration.apiCallAttemptTimeout().isPresent());
    }

    private String getAdvancedOption(
            ClientOverrideConfiguration configuration,
            SdkAdvancedClientOption<String> userAgentPrefix) {
        return configuration.toBuilder().advancedOptions().toBuilder().get(userAgentPrefix);
    }
}
