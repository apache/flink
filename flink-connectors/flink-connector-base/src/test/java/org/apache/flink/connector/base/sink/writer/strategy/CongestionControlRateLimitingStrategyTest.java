/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer.strategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test class for {@link CongestionControlRateLimitingStrategy}. */
class CongestionControlRateLimitingStrategyTest {
    @Test
    void testMaxInFlightRequestsRespected() {
        final int maxInFlightRequests = 2;
        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setInitialMaxInFlightMessages(10)
                        .setScalingStrategy(AIMDScalingStrategy.builder(10).build())
                        .build();

        final RequestInfo emptyRequest = new BasicRequestInfo(0);
        final ResultInfo emptyResult = new BasicResultInfo(0, 0);

        for (int i = 0; i < maxInFlightRequests; i++) {
            assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
            strategy.registerInFlightRequest(emptyRequest);
        }
        assertThat(strategy.shouldBlock(emptyRequest)).isTrue();

        strategy.registerCompletedRequest(emptyResult);
        assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
    }

    @Test
    void testMaxInFlightRequestsDoesNotGoBelowZero() {
        final int maxInFlightRequests = 1;
        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setInitialMaxInFlightMessages(10)
                        .setScalingStrategy(AIMDScalingStrategy.builder(10).build())
                        .build();

        final RequestInfo emptyRequest = new BasicRequestInfo(0);
        final ResultInfo emptyResult = new BasicResultInfo(0, 0);

        // Register completed request before ever starting a request
        strategy.registerCompletedRequest(emptyResult);
        for (int i = 0; i < maxInFlightRequests; i++) {
            assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
            strategy.registerInFlightRequest(emptyRequest);
        }
        assertThat(strategy.shouldBlock(emptyRequest)).isTrue();

        strategy.registerCompletedRequest(emptyResult);
        assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
    }

    @Test
    void testInitialMaxInFlightMessagesRespected() {
        final RequestInfo requestWithTwoMessages = new BasicRequestInfo(2);
        final ResultInfo resultWithTwoMessages = new BasicResultInfo(0, 2);

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(4)
                        .setScalingStrategy(
                                AIMDScalingStrategy.builder(4).setIncreaseRate(1).build())
                        .build();

        strategy.registerInFlightRequest(requestWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();

        strategy.registerInFlightRequest(requestWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();

        strategy.registerCompletedRequest(resultWithTwoMessages);
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();
    }

    @Test
    void testAimdScalingStrategyScaleUpOnSuccess() {
        final RequestInfo emptyRequest = new BasicRequestInfo(0);
        final ResultInfo emptySuccessfulResult = new BasicResultInfo(0, 0);
        final BasicRequestInfo requestWithTwoMessages = new BasicRequestInfo(2);

        AIMDScalingStrategy aimdScalingStrategy =
                AIMDScalingStrategy.builder(100).setIncreaseRate(10).setDecreaseFactor(0.5).build();

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(1)
                        .setScalingStrategy(aimdScalingStrategy)
                        .build();

        assertThat(strategy.shouldBlock(emptyRequest)).isFalse();
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();

        strategy.registerInFlightRequest(emptyRequest);
        strategy.registerCompletedRequest(emptySuccessfulResult);

        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();
    }

    @Test
    void testAimdScalingStrategyScaleDownOnFailure() {
        final RequestInfo requestWithOneMessage = new BasicRequestInfo(1);
        final ResultInfo resultWithOneFailedMessage = new BasicResultInfo(1, 1);
        final RequestInfo requestWithTwoMessages = new BasicRequestInfo(2);
        AIMDScalingStrategy aimdScalingStrategy =
                AIMDScalingStrategy.builder(100).setDecreaseFactor(0.5).build();

        CongestionControlRateLimitingStrategy strategy =
                CongestionControlRateLimitingStrategy.builder()
                        .setMaxInFlightRequests(100)
                        .setInitialMaxInFlightMessages(2)
                        .setScalingStrategy(aimdScalingStrategy)
                        .build();

        assertThat(strategy.shouldBlock(requestWithOneMessage)).isFalse();
        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isFalse();

        strategy.registerInFlightRequest(requestWithOneMessage);
        strategy.registerCompletedRequest(resultWithOneFailedMessage);

        assertThat(strategy.shouldBlock(requestWithTwoMessages)).isTrue();
    }

    @Test
    void testInvalidMaxInFlightRequests() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(0)
                                        .setInitialMaxInFlightMessages(10)
                                        .setScalingStrategy(AIMDScalingStrategy.builder(10).build())
                                        .build())
                .withMessageContaining("maxInFlightRequests must be a positive integer.");
    }

    @Test
    void testInvalidMaxInFlightMessages() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(10)
                                        .setInitialMaxInFlightMessages(0)
                                        .setScalingStrategy(AIMDScalingStrategy.builder(10).build())
                                        .build())
                .withMessageContaining("initialMaxInFlightMessages must be a positive integer.");
    }

    @Test
    void testInvalidAimdStrategy() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                CongestionControlRateLimitingStrategy.builder()
                                        .setMaxInFlightRequests(10)
                                        .setInitialMaxInFlightMessages(10)
                                        .build())
                .withMessageContaining("scalingStrategy must be provided.");
    }
}
