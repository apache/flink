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

package org.apache.flink.api.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** Tests for {@link RestartStrategyDescriptionUtils} */
public class RestartStrategyDescriptionUtilsTest {

    private Configuration configuration;

    @BeforeEach
    public void setUp() {
        configuration = new Configuration();
    }

    @Test
    public void testGetNoRestartStrategyDescription() {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        String description =
                RestartStrategyDescriptionUtils.getRestartStrategyDescription(configuration);
        assertThat("Restart deactivated.").isEqualTo(description);
    }

    @Test
    public void testGetFixedDelayDescription() {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));

        String description =
                RestartStrategyDescriptionUtils.getRestartStrategyDescription(configuration);
        assertThat("Restart with fixed delay (PT10S). #3 restart attempts.").isEqualTo(description);
    }

    @Test
    public void testGetFailureRateDescription() {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 5);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL,
                Duration.ofMinutes(1));
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(15));

        String description =
                RestartStrategyDescriptionUtils.getRestartStrategyDescription(configuration);
        assertThat(
                        "Failure rate restart with maximum of 5 failures within interval PT1M and fixed delay PT15S.")
                .isEqualTo(description);
    }

    @Test
    public void testGetExponentialDelayDescription() {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF,
                Duration.ofSeconds(1));
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF,
                Duration.ofMinutes(2));
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD,
                Duration.ofMinutes(5));
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.5);

        String description =
                RestartStrategyDescriptionUtils.getRestartStrategyDescription(configuration);
        assertThat(
                        "Restart with exponential delay: starting at PT1S, increasing by 2.000000, with maximum PT2M. "
                                + "Delay resets after PT5M with jitter 0.500000")
                .isEqualTo(description);
    }

    @Test
    public void testGetDefaultRestartStrategyDescription() {
        String description =
                RestartStrategyDescriptionUtils.getRestartStrategyDescription(configuration);
        assertThat("Cluster level default restart strategy").isEqualTo(description);
    }

    @Test
    public void testGetRestartStrategyDescriptionForUnknownStrategy() {
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "unknown");

        assertThatThrownBy(
                        () ->
                                RestartStrategyDescriptionUtils.getRestartStrategyDescription(
                                        configuration))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
