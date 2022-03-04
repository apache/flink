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

package org.apache.flink.connector.base.sink.writer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Unit tests for {@link AIMDRateLimitingStrategy}. */
public class AIMDRateLimitingStrategyTest {
    private static final int INITIAL_RATE = 32;
    private static final int INCREASE_RATE = 10;
    private static final int THRESHOLD = 100;
    private static final double DECREASE_RATE = 0.5;

    @Test
    public void testInitialRateIsSetByConstructor() {
        AIMDRateLimitingStrategy rateLimitingStrategy =
                new AIMDRateLimitingStrategy(INCREASE_RATE, DECREASE_RATE, THRESHOLD, INITIAL_RATE);
        assertThat(rateLimitingStrategy.getRateLimit()).isEqualTo(INITIAL_RATE);
    }

    @Test
    public void testAcknowledgedRequestsIncreaseLinearly() {
        AIMDRateLimitingStrategy rateLimitingStrategy =
                new AIMDRateLimitingStrategy(INCREASE_RATE, DECREASE_RATE, THRESHOLD, INITIAL_RATE);

        int numberOfAcks = 5;
        updateStrategyWithAcks(rateLimitingStrategy, numberOfAcks);

        assertThat(rateLimitingStrategy.getRateLimit())
                .isEqualTo(INITIAL_RATE + numberOfAcks * INCREASE_RATE);
    }

    @Test
    public void testFailedRequestsDecreaseExponentially() {
        AIMDRateLimitingStrategy rateLimitingStrategy =
                new AIMDRateLimitingStrategy(INCREASE_RATE, DECREASE_RATE, THRESHOLD, INITIAL_RATE);
        int numberOfFailures = 5;
        int rate = rateLimitingStrategy.getRateLimit();

        for (int i = 1; i < numberOfFailures; i++) {
            rateLimitingStrategy.scaleDown();

            assertThat(rateLimitingStrategy.getRateLimit())
                    .isEqualTo(decreaseRateWithFactor(rate, DECREASE_RATE));

            rate = decreaseRateWithFactor(rate, DECREASE_RATE);
        }
    }

    @Test
    public void testIncreaseRateNeverExceedsThreshold() {
        AIMDRateLimitingStrategy rateLimitingStrategy =
                new AIMDRateLimitingStrategy(INCREASE_RATE, DECREASE_RATE, THRESHOLD, INITIAL_RATE);

        int numberOfAcks = THRESHOLD / INCREASE_RATE + 1;
        updateStrategyWithAcks(rateLimitingStrategy, numberOfAcks);

        assertThat(rateLimitingStrategy.getRateLimit()).isEqualTo(THRESHOLD);
    }

    @Test
    public void testFailureOnInitialBiggerThanThreshold() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new AIMDRateLimitingStrategy(10, 0.5, 10, 11))
                .withMessageContaining("Initial rate must not exceed threshold");
    }

    @Test
    public void testFailureOnInvalidDecreaseFactor() {
        double badDecreaseFactor = 1.5;
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new AIMDRateLimitingStrategy(10, badDecreaseFactor, 100, 11))
                .withMessageContaining("Decrease factor must be between 0.0 and 1.0");
    }

    @Test
    public void testFailureOnInvalidIncreaseRate() {
        int badIncreaseRate = -15;
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new AIMDRateLimitingStrategy(badIncreaseRate, 0.5, 100, 11))
                .withMessageContaining("Increase rate must be positive integer.");
    }

    private void updateStrategyWithAcks(
            AIMDRateLimitingStrategy rateLimitingStrategy, int numberOfAcks) {
        for (int i = 0; i < numberOfAcks; i++) {
            rateLimitingStrategy.scaleUp();
        }
    }

    private int decreaseRateWithFactor(int rate, double factor) {
        return Math.max((int) Math.round(rate * factor), 1);
    }
}
