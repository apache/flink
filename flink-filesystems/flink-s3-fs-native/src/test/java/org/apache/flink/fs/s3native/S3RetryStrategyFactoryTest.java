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

import org.apache.flink.fs.s3native.NativeS3FileSystemFactory.RetryMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.api.RetryToken;
import software.amazon.awssdk.retries.api.TokenAcquisitionFailedException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3RetryStrategyFactory}. */
class S3RetryStrategyFactoryTest {

    private static RetryStrategy create(RetryMode mode, boolean circuitBreakerEnabled) {
        return S3RetryStrategyFactory.create(
                mode,
                10,
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                Duration.ofSeconds(20),
                circuitBreakerEnabled);
    }

    /**
     * The strategy as the SDK client completes it: the client adds the conditions that say what is
     * retried, here everything.
     */
    private static RetryStrategy retryingEverything(RetryStrategy strategy) {
        RetryStrategy.Builder<?, ?> builder = strategy.toBuilder();
        builder.retryOnException(failure -> true);
        return builder.build();
    }

    /** One call whose attempts fail up to the last one: all ten retries of the strategy. */
    private static void retryTenTimes(RetryStrategy strategy) {
        SdkClientException failure = SdkClientException.create("boom");
        RetryToken token =
                strategy.acquireInitialToken(AcquireInitialTokenRequest.create("test")).token();
        for (int retry = 0; retry < 10; retry++) {
            token =
                    strategy.refreshRetryToken(
                                    RefreshRetryTokenRequest.builder()
                                            .token(token)
                                            .failure(failure)
                                            .build())
                            .token();
        }
    }

    @Test
    void testStandardModeBuildsTheStandardStrategy() {
        assertThat(create(RetryMode.STANDARD, false)).isInstanceOf(StandardRetryStrategy.class);
    }

    @Test
    void testAdaptiveModeBuildsTheAdaptiveStrategy() {
        assertThat(create(RetryMode.ADAPTIVE, false)).isInstanceOf(AdaptiveRetryStrategy.class);
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testMaxAttemptsIsMaxRetriesPlusOne(RetryMode mode) {
        assertThat(create(mode, false).maxAttempts()).isEqualTo(11);
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testCircuitBreakerStaysOffUnlessEnabled(RetryMode mode) {
        RetryStrategy strategy = retryingEverything(create(mode, false));

        // Ten retries cost 50 tokens of the 500 of the breaker: it would be empty after ten calls.
        for (int call = 0; call < 30; call++) {
            retryTenTimes(strategy);
        }
    }

    @ParameterizedTest
    @EnumSource(RetryMode.class)
    void testCircuitBreakerStopsTheRetriesWhenEnabled(RetryMode mode) {
        RetryStrategy strategy = retryingEverything(create(mode, true));

        assertThatThrownBy(
                        () -> {
                            for (int call = 0; call < 30; call++) {
                                retryTenTimes(strategy);
                            }
                        })
                .isInstanceOf(TokenAcquisitionFailedException.class)
                .hasMessageContaining("protect the caller");
    }
}
