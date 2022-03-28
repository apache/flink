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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.configuration.CleanupOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code CleanupRetryStrategyFactoryTest} tests {@link CleanupRetryStrategyFactory}. */
class CleanupRetryStrategyFactoryTest {

    private static final CleanupRetryStrategyFactory TEST_INSTANCE =
            CleanupRetryStrategyFactory.INSTANCE;

    @ParameterizedTest
    @ValueSource(strings = {"none", "disable", "off", "NONE", "None", "oFf"})
    public void testNoRetryStrategyCreation(String configValue) {
        final Configuration config = createConfigurationWithRetryStrategy(configValue);
        final RetryStrategy retryStrategy = TEST_INSTANCE.createRetryStrategy(config);

        assertThat(retryStrategy.getRetryDelay()).isZero();
        assertThat(retryStrategy.getNumRemainingRetries()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "random string"})
    public void testInvalidConfiguration(String value) {
        assertThatThrownBy(
                () ->
                        TEST_INSTANCE.createRetryStrategy(
                                createConfigurationWithRetryStrategy(value)));
    }

    @Test
    public void testDefaultStrategyCreation() {
        testExponentialBackoffDelayRetryStrategyCreation(
                new Configuration(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS.defaultValue());
    }

    private static Configuration createConfigurationWithRetryStrategy(String configValue) {
        final Configuration config = new Configuration();
        config.set(CleanupOptions.CLEANUP_STRATEGY, configValue);

        return config;
    }

    /* *********************************************
     * FixedDelayRetryStrategy tests
     * *********************************************/

    @Test
    public void testFixedDelayStrategyCreationWithFixedDelayStrategyDefaultLabel() {
        testFixedDelayStrategyWithDefaultValues(CleanupOptions.FIXED_DELAY_LABEL);
    }

    @Test
    public void testFixedDelayStrategyCreationWithAlphaNumericFixedDelayStrategyLabel() {
        testFixedDelayStrategyWithDefaultValues(
                CleanupOptions.extractAlphaNumericCharacters(CleanupOptions.FIXED_DELAY_LABEL));
    }

    private static void testFixedDelayStrategyWithDefaultValues(String label) {
        final Configuration config = createConfigurationWithRetryStrategy(label);
        testFixedDelayStrategyCreation(
                config,
                CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_DELAY.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS.defaultValue());
    }

    @Test
    public void testFixedDelayStrategyWithCustomDelay() {
        final Configuration config =
                createConfigurationWithRetryStrategy(CleanupOptions.FIXED_DELAY_LABEL);
        final Duration customDelay =
                CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_DELAY.defaultValue().plusMinutes(1);
        config.set(CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_DELAY, customDelay);

        testFixedDelayStrategyCreation(
                config,
                customDelay,
                CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS.defaultValue());
    }

    @Test
    public void testFixedDelayStrategyWithCustomMaxAttempts() {
        final Configuration config =
                createConfigurationWithRetryStrategy(CleanupOptions.FIXED_DELAY_LABEL);
        final int customMaxAttempts = 1;
        Preconditions.checkArgument(
                customMaxAttempts
                        != CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS.defaultValue(),
                "The custom value should be different from the default value to make it possible that the overwritten value is selected.");
        config.set(CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS, customMaxAttempts);

        testFixedDelayStrategyCreation(
                config,
                CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_DELAY.defaultValue(),
                customMaxAttempts);
    }

    private static void testFixedDelayStrategyCreation(
            Configuration config, Duration expectedDelay, int expectedMaxAttempts) {
        final RetryStrategy retryStrategy = TEST_INSTANCE.createRetryStrategy(config);

        assertThat(retryStrategy).isInstanceOf(FixedRetryStrategy.class);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(expectedDelay);
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(expectedMaxAttempts);
    }
    /* *********************************************
     * ExponentialBackoffDelayRetryStrategy tests
     * *********************************************/

    @Test
    public void
            testExponentialBackoffDelayRetryStrategyCreationWithFixedDelayStrategyDefaultLabel() {
        testExponentialBackoffDelayRetryStrategyWithDefaultValues(
                CleanupOptions.EXPONENTIAL_DELAY_LABEL);
    }

    @Test
    public void
            testExponentialBackoffDelayRetryStrategyCreationWithAlphaNumericFixedDelayStrategyLabel() {
        testExponentialBackoffDelayRetryStrategyWithDefaultValues(
                CleanupOptions.extractAlphaNumericCharacters(
                        CleanupOptions.EXPONENTIAL_DELAY_LABEL));
    }

    private static void testExponentialBackoffDelayRetryStrategyWithDefaultValues(String label) {
        final Configuration config = createConfigurationWithRetryStrategy(label);
        testExponentialBackoffDelayRetryStrategyCreation(
                config,
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS.defaultValue());
    }

    @Test
    public void testExponentialBackoffDelayRetryStrategyWithCustomMinimumDelay() {
        final Configuration config =
                createConfigurationWithRetryStrategy(CleanupOptions.EXPONENTIAL_DELAY_LABEL);
        final Duration customMinDelay =
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                        .defaultValue()
                        .plusMinutes(1);
        config.set(
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, customMinDelay);

        testExponentialBackoffDelayRetryStrategyCreation(
                config,
                customMinDelay,
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS.defaultValue());
    }

    @Test
    public void testExponentialBackoffDelayRetryStrategyWithCustomMaximumDelay() {
        final Configuration config =
                createConfigurationWithRetryStrategy(CleanupOptions.EXPONENTIAL_DELAY_LABEL);
        final Duration customMaxDelay =
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF
                        .defaultValue()
                        .plusMinutes(1);
        config.set(CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, customMaxDelay);

        testExponentialBackoffDelayRetryStrategyCreation(
                config,
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF.defaultValue(),
                customMaxDelay,
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS.defaultValue());
    }

    @Test
    public void testExponentialBackoffDelayRetryStrategyWithCustomMaxAttempts() {
        final Configuration config =
                createConfigurationWithRetryStrategy(CleanupOptions.EXPONENTIAL_DELAY_LABEL);
        // 13 is the minimum we can use for this test; otherwise, assertMaxDelay would fail due to a
        // Precondition in ExponentialBackoffRetryStrategy
        final int customMaxAttempts = 13;
        Preconditions.checkArgument(
                customMaxAttempts
                        != CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS
                                .defaultValue(),
                "The custom value should be different from the default value to make it possible that the overwritten value is selected.");
        config.set(
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS, customMaxAttempts);

        testExponentialBackoffDelayRetryStrategyCreation(
                config,
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF.defaultValue(),
                CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF.defaultValue(),
                customMaxAttempts);
    }

    private static void testExponentialBackoffDelayRetryStrategyCreation(
            Configuration config,
            Duration expectedMinDelay,
            Duration expectedMaxDelay,
            int expectedMaxAttempts) {
        final RetryStrategy retryStrategy = TEST_INSTANCE.createRetryStrategy(config);

        assertThat(retryStrategy).isInstanceOf(ExponentialBackoffRetryStrategy.class);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(expectedMinDelay);
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(expectedMaxAttempts);

        assertMaxDelay(retryStrategy, expectedMaxDelay);
    }

    private static void assertMaxDelay(RetryStrategy retryStrategy, Duration expectedMaximumDelay) {
        RetryStrategy nextInstance = retryStrategy.getNextRetryStrategy();
        Duration currentDuration = retryStrategy.getRetryDelay();
        while (!nextInstance.getRetryDelay().equals(currentDuration)) {
            currentDuration = nextInstance.getRetryDelay();
            nextInstance = nextInstance.getNextRetryStrategy();
        }

        assertThat(currentDuration).isEqualTo(expectedMaximumDelay);
    }
}
