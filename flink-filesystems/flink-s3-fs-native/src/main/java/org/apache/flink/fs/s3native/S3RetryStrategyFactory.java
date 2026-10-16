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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.internal.DefaultAdaptiveRetryStrategy;

import java.time.Duration;
import java.util.Arrays;

/**
 * Builds the AWS SDK {@link RetryStrategy} used by the native S3 client's sync/async clients: the
 * standard or the adaptive one, see {@link NativeS3FileSystemFactory#RETRY_MODE}. The delays are
 * validated by {@link S3ClientProvider.Builder}.
 */
@Internal
final class S3RetryStrategyFactory {

    private S3RetryStrategyFactory() {}

    static RetryStrategy create(
            NativeS3FileSystemFactory.RetryMode retryMode,
            int maxRetries,
            Duration retryBaseDelay,
            Duration retryThrottleBaseDelay,
            Duration retryMaxBackoff,
            boolean retryCircuitBreakerEnabled) {
        BackoffStrategy backoff = BackoffStrategy.exponentialDelay(retryBaseDelay, retryMaxBackoff);
        BackoffStrategy throttlingBackoff =
                BackoffStrategy.exponentialDelay(retryThrottleBaseDelay, retryMaxBackoff);

        switch (retryMode) {
            case STANDARD:
                return StandardRetryStrategy.builder()
                        .maxAttempts(maxRetries + 1)
                        .backoffStrategy(backoff)
                        .throttlingBackoffStrategy(throttlingBackoff)
                        .circuitBreakerEnabled(retryCircuitBreakerEnabled)
                        .build();
            case ADAPTIVE:
                return createAdaptive(
                        maxRetries, backoff, throttlingBackoff, retryCircuitBreakerEnabled);
            default:
                throw new IllegalArgumentException(
                        "Unknown retry mode: "
                                + retryMode
                                + ". Supported values: "
                                + Arrays.toString(NativeS3FileSystemFactory.RetryMode.values()));
        }
    }

    private static RetryStrategy createAdaptive(
            int maxRetries,
            BackoffStrategy backoff,
            BackoffStrategy throttlingBackoff,
            boolean retryCircuitBreakerEnabled) {
        AdaptiveRetryStrategy.Builder builder =
                AdaptiveRetryStrategy.builder()
                        .maxAttempts(maxRetries + 1)
                        .backoffStrategy(backoff)
                        .throttlingBackoffStrategy(throttlingBackoff);
        // The public builder of the adaptive strategy has no switch for the circuit breaker, so it
        // would stay on and a throttling storm would drain its token bucket within seconds (see
        // RETRY_CIRCUIT_BREAKER_ENABLED). The SDK's own builder has the switch.
        ((DefaultAdaptiveRetryStrategy.Builder) builder)
                .circuitBreakerEnabled(retryCircuitBreakerEnabled);
        return builder.build();
    }
}
