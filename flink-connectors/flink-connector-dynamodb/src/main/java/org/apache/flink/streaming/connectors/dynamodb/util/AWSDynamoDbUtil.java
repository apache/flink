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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.dynamodb.config.AWSDynamoDbConfigConstants;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

/** Some utilities specific to Amazon Web Service. */
public class AWSDynamoDbUtil extends AWSGeneralUtil {

    public static DynamoDbAsyncClient createClient(final Properties properties) {
        DynamoDbAsyncClientBuilder clientBuilder = DynamoDbAsyncClient.builder();

        if (properties.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
            final URI endpointOverride =
                    URI.create(properties.getProperty(AWSConfigConstants.AWS_ENDPOINT));
            clientBuilder.endpointOverride(endpointOverride);
        }

        return clientBuilder
                .httpClient(createAsyncHttpClient(properties))
                .region(getRegion(properties))
                .credentialsProvider(getCredentialsProvider(properties))
                .overrideConfiguration(getOverrideConfiguration(properties))
                .build();
    }

    @VisibleForTesting
    static ClientOverrideConfiguration getOverrideConfiguration(Properties properties) {
        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();
        SdkClientConfiguration config = SdkClientConfiguration.builder().build();

        builder.putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_PREFIX,
                        getFlinkUserAgentPrefix(properties))
                .putAdvancedOption(
                        SdkAdvancedClientOption.USER_AGENT_SUFFIX,
                        config.option(SdkAdvancedClientOption.USER_AGENT_SUFFIX));

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_ATTEMPT_TIMEOUT))
                .ifPresent(builder::apiCallAttemptTimeout);

        Optional.ofNullable(config.option(SdkClientOption.API_CALL_TIMEOUT))
                .ifPresent(builder::apiCallTimeout);

        builder.retryPolicy(getRetryPolicy(properties));
        return builder.build();
    }

    @VisibleForTesting
    static String getFlinkUserAgentPrefix(Properties properties) {
        if (properties.containsKey(AWSDynamoDbConfigConstants.DYNAMODB_CLIENT_USER_AGENT_PREFIX)) {
            return properties.getProperty(
                    AWSDynamoDbConfigConstants.DYNAMODB_CLIENT_USER_AGENT_PREFIX);
        }
        return String.format(
                AWSDynamoDbConfigConstants.BASE_DYNAMODB_USER_AGENT_PREFIX_FORMAT,
                EnvironmentInformation.getVersion(),
                EnvironmentInformation.getRevisionInformation().commitId);
    }

    @VisibleForTesting
    static RetryPolicy getRetryPolicy(Properties properties) {
        if (hasRetryConfiguration(properties)) {
            RetryPolicy.Builder builder = RetryPolicy.builder();

            if (properties.containsKey(AWSDynamoDbConfigConstants.NUMBER_RETRIES)) {
                builder.numRetries(
                        Integer.parseInt(
                                properties.getProperty(AWSDynamoDbConfigConstants.NUMBER_RETRIES)));
            }

            if (properties.containsKey(AWSDynamoDbConfigConstants.BACKOFF_STRATEGY)) {
                builder.backoffStrategy(
                        getBackoffStrategy(
                                properties, AWSDynamoDbConfigConstants.BACKOFF_STRATEGY));
            }

            if (properties.containsKey(AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY)) {
                builder.throttlingBackoffStrategy(
                        getBackoffStrategy(
                                properties,
                                AWSDynamoDbConfigConstants.THROTTLING_BACKOFF_STRATEGY));
            }
            return builder.build();
        }
        return RetryPolicy.defaultRetryPolicy();
    }

    @VisibleForTesting
    static boolean hasRetryConfiguration(Properties properties) {
        return properties.stringPropertyNames().stream()
                .anyMatch(
                        name ->
                                name.startsWith(
                                        AWSDynamoDbConfigConstants
                                                .AWS_DYNAMODB_CLIENT_RETRY_PREFIX));
    }

    @VisibleForTesting
    static BackoffStrategy getBackoffStrategy(Properties properties, String strategy) {
        AWSDynamoDbConfigConstants.BackoffStrategy backoffStrategy =
                AWSDynamoDbConfigConstants.BackoffStrategy.valueOf(
                        properties.getProperty(strategy));

        switch (backoffStrategy) {
            case FULL_JITTER:
                return FullJitterBackoffStrategy.builder()
                        .baseDelay(
                                getDuration(
                                        properties.getProperty(
                                                AWSDynamoDbConfigConstants
                                                        .FULL_JITTER_BASE_DELAY_MS)))
                        .maxBackoffTime(
                                getDuration(
                                        properties.getProperty(
                                                AWSDynamoDbConfigConstants
                                                        .FULL_JITTER_MAX_BACKOFF_TIME_MS)))
                        .build();
            case EQUAL_JITTER:
                return EqualJitterBackoffStrategy.builder()
                        .baseDelay(
                                getDuration(
                                        properties.getProperty(
                                                AWSDynamoDbConfigConstants
                                                        .EQUAL_JITTER_BASE_DELAY_MS)))
                        .maxBackoffTime(
                                getDuration(
                                        properties.getProperty(
                                                AWSDynamoDbConfigConstants
                                                        .EQUAL_JITTER_MAX_BACKOFF_TIME_MS)))
                        .build();
            case FIXED_DELAY:
                return FixedDelayBackoffStrategy.create(
                        getDuration(
                                properties.getProperty(
                                        AWSDynamoDbConfigConstants.FIXED_DELAY_BACKOFF_MS)));
            default:
                return BackoffStrategy.defaultStrategy();
        }
    }

    private static Duration getDuration(String timeInMillis) {
        return Duration.ofMillis(Integer.parseInt(timeInMillis));
    }
}
