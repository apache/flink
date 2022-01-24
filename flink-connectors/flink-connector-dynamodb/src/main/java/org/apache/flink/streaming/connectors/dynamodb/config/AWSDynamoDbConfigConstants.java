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

package org.apache.flink.streaming.connectors.dynamodb.config;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.dynamodb.util.AWSDynamoDbUtil;

import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;

/** Defaults for {@link AWSDynamoDbUtil}. */
@PublicEvolving
public class AWSDynamoDbConfigConstants {

    public static final String BASE_DYNAMODB_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) DynamoDB Connector";

    /** Identifier for user agent prefix. */
    public static final String DYNAMODB_CLIENT_USER_AGENT_PREFIX =
            "aws.dynamodb.client.user-agent-prefix";

    public static final String AWS_DYNAMODB_CLIENT_RETRY_PREFIX = "aws.dynamodb.client.retry";

    /**
     * Possible configuration values for backoff strategy on retry when writing to DynamoDB.
     * Internally, a corresponding implementation of {@link
     * software.amazon.awssdk.core.retry.backoff.BackoffStrategy} will be used.
     */
    public enum BackoffStrategy {

        /**
         * Backoff strategy that uses a full jitter strategy for computing the next backoff delay. A
         * full jitter strategy will always compute a new random delay between and the computed
         * exponential backoff for each subsequent request. See example: {@link
         * FullJitterBackoffStrategy}
         */
        FULL_JITTER,

        /**
         * Backoff strategy that uses equal jitter for computing the delay before the next retry. An
         * equal jitter backoff strategy will first compute an exponential delay based on the
         * current number of retries, base delay and max delay. The final computed delay before the
         * next retry will keep half of this computed delay plus a random delay computed as a random
         * number between 0 and half of the exponential delay plus one. See example: {@link
         * EqualJitterBackoffStrategy}
         */
        EQUAL_JITTER,

        /**
         * Simple backoff strategy that always uses a fixed delay for the delay * before the next
         * retry attempt. See example: {@link FixedDelayBackoffStrategy}
         */
        FIXED_DELAY
    }

    /**
     * The maximum number of times that a single request should be retried by the DynamoDB client,
     * assuming it fails for a retryable error.
     */
    public static final String NUMBER_RETRIES =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".number-retries";

    /**
     * Backoff strategy when writing to DynamoDb on retries. Supported values: {@link
     * BackoffStrategy}.
     */
    public static final String BACKOFF_STRATEGY =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".backoff-strategy";

    /**
     * Configure the backoff strategy that should be used for waiting in between retry attempts
     * after a throttling error is encountered. If the retry is not because of throttling reasons,
     * BACKOFF_STRATEGY is used. Supported values: {@link BackoffStrategy}.
     */
    public static final String THROTTLING_BACKOFF_STRATEGY =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".throttling-backoff-strategy";

    public static final String FULL_JITTER_BASE_DELAY_MS =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".full-jitter.base-delay-ms";
    public static final String FULL_JITTER_MAX_BACKOFF_TIME_MS =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".full-jitter.max-backoff-time-ms";

    public static final String EQUAL_JITTER_BASE_DELAY_MS =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".equal-jitter.base-delay-ms";
    public static final String EQUAL_JITTER_MAX_BACKOFF_TIME_MS =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".equal-jitter.max-backoff-time-ms";

    public static final String FIXED_DELAY_BACKOFF_MS =
            AWS_DYNAMODB_CLIENT_RETRY_PREFIX + ".fixed-delay.fixed-backoff-time-ms";
}
