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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RestartBackoffTimeStrategyFactoryLoader}. */
class RestartBackoffTimeStrategyFactoryLoaderTest {

    private static final RestartStrategies.RestartStrategyConfiguration
            DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION = RestartStrategies.fallBackRestart();

    @Test
    void testNoRestartStrategySpecifiedInJobConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.noRestart(), conf, false);

        assertThat(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE)
                .isEqualTo(factory);
    }

    @Test
    void testFixedDelayRestartStrategySpecifiedInJobConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.fixedDelayRestart(1, Time.milliseconds(1000)),
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        FixedDelayRestartBackoffTimeStrategy
                                .FixedDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testExponentialDelayRestartStrategySpecifiedInJobConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.exponentialDelayRestart(
                                Time.milliseconds(1),
                                Time.milliseconds(1000),
                                1.1,
                                Time.milliseconds(2000),
                                0),
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        ExponentialDelayRestartBackoffTimeStrategy
                                .ExponentialDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testFailureRateRestartStrategySpecifiedInJobConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.failureRateRestart(
                                1, Time.milliseconds(1000), Time.milliseconds(1000)),
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        FailureRateRestartBackoffTimeStrategy
                                .FailureRateRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testNoRestartStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "none");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, conf, false);

        assertThat(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE)
                .isEqualTo(factory);
    }

    @Test
    void testFixedDelayStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, conf, false);

        assertThat(factory)
                .isInstanceOf(
                        FixedDelayRestartBackoffTimeStrategy
                                .FixedDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testExponentialDelayStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, conf, false);

        assertThat(factory)
                .isInstanceOf(
                        ExponentialDelayRestartBackoffTimeStrategy
                                .ExponentialDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testFailureRateStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, conf, false);

        assertThat(factory)
                .isInstanceOf(
                        FailureRateRestartBackoffTimeStrategy
                                .FailureRateRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testInvalidStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.setString(RestartStrategyOptions.RESTART_STRATEGY, "invalid-strategy");

        assertThatThrownBy(
                        () ->
                                RestartBackoffTimeStrategyFactoryLoader
                                        .createRestartBackoffTimeStrategyFactory(
                                                DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
                                                conf,
                                                false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNoStrategySpecifiedWhenCheckpointingEnabled() {
        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), true);

        RestartBackoffTimeStrategy strategy = factory.create();
        assertThat(strategy).isInstanceOf(FixedDelayRestartBackoffTimeStrategy.class);

        FixedDelayRestartBackoffTimeStrategy fixedDelayStrategy =
                (FixedDelayRestartBackoffTimeStrategy) strategy;
        assertThat(RestartBackoffTimeStrategyFactoryLoader.DEFAULT_RESTART_DELAY)
                .isEqualTo(fixedDelayStrategy.getBackoffTime());
        assertThat(RestartBackoffTimeStrategyFactoryLoader.DEFAULT_RESTART_ATTEMPTS)
                .isEqualTo(fixedDelayStrategy.getMaxNumberRestartAttempts());
    }

    @Test
    void testNoStrategySpecifiedWhenCheckpointingDisabled() {
        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), false);

        assertThat(factory)
                .isInstanceOf(
                        NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.class);
    }
}
