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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.EXPONENTIAL_DELAY;
import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.FAILURE_RATE;
import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.FIXED_DELAY;
import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RestartBackoffTimeStrategyFactoryLoader}. */
class RestartBackoffTimeStrategyFactoryLoaderTest {

    private static final RestartStrategies.RestartStrategyConfiguration
            DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION = RestartStrategies.fallBackRestart();

    @Test
    void testNoRestartStrategySpecifiedInExecutionConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.noRestart(), conf, conf, false);

        assertThat(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE)
                .isEqualTo(factory);
    }

    @Test
    void testFixedDelayRestartStrategySpecifiedInExecutionConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.fixedDelayRestart(1, Time.milliseconds(1000)),
                        conf,
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        FixedDelayRestartBackoffTimeStrategy
                                .FixedDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testExponentialDelayRestartStrategySpecifiedInExecutionConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.exponentialDelayRestart(
                                Duration.ofMillis(1),
                                Duration.ofMillis(1000),
                                1.1,
                                Duration.ofMillis(2000),
                                0),
                        conf,
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        ExponentialDelayRestartBackoffTimeStrategy
                                .ExponentialDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testFailureRateRestartStrategySpecifiedInExecutionConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        RestartStrategies.failureRateRestart(
                                1, Duration.ofMillis(1000), Duration.ofMillis(1000)),
                        conf,
                        conf,
                        false);

        assertThat(factory)
                .isInstanceOf(
                        FailureRateRestartBackoffTimeStrategy
                                .FailureRateRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testNoRestartStrategySpecifiedInJobConfig() {
        final Configuration jobConf = new Configuration();
        jobConf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        final Configuration clusterConf = new Configuration();
        clusterConf.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, jobConf, clusterConf, false);

        assertThat(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE)
                .isEqualTo(factory);
    }

    @Test
    void testFixedDelayStrategySpecifiedInJobConfig() {
        final Configuration jobConf = new Configuration();
        jobConf.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        final Configuration clusterConf = new Configuration();
        clusterConf.set(RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, jobConf, clusterConf, false);

        assertThat(factory)
                .isInstanceOf(
                        FixedDelayRestartBackoffTimeStrategy
                                .FixedDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testExponentialDelayStrategySpecifiedInJobConfig() {
        final Configuration jobConf = new Configuration();
        jobConf.set(RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());
        final Configuration clusterConf = new Configuration();
        clusterConf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, jobConf, clusterConf, false);

        assertThat(factory)
                .isInstanceOf(
                        ExponentialDelayRestartBackoffTimeStrategy
                                .ExponentialDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testFailureRateStrategySpecifiedInJobConfig() {
        final Configuration jobConf = new Configuration();
        jobConf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());
        final Configuration clusterConf = new Configuration();
        clusterConf.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, jobConf, clusterConf, false);

        assertThat(factory)
                .isInstanceOf(
                        FailureRateRestartBackoffTimeStrategy
                                .FailureRateRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testInvalidStrategySpecifiedInJobConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "invalid-strategy");

        assertThatThrownBy(
                        () ->
                                RestartBackoffTimeStrategyFactoryLoader
                                        .createRestartBackoffTimeStrategyFactory(
                                                DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
                                                conf,
                                                new Configuration(),
                                                false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNoRestartStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), conf, false);

        assertThat(NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE)
                .isEqualTo(factory);
    }

    @Test
    void testFixedDelayStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), conf, false);

        assertThat(factory)
                .isInstanceOf(
                        FixedDelayRestartBackoffTimeStrategy
                                .FixedDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testExponentialDelayStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, EXPONENTIAL_DELAY.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), conf, false);

        assertThat(factory)
                .isInstanceOf(
                        ExponentialDelayRestartBackoffTimeStrategy
                                .ExponentialDelayRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testFailureRateStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, FAILURE_RATE.getMainValue());

        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION, new Configuration(), conf, false);

        assertThat(factory)
                .isInstanceOf(
                        FailureRateRestartBackoffTimeStrategy
                                .FailureRateRestartBackoffTimeStrategyFactory.class);
    }

    @Test
    void testInvalidStrategySpecifiedInClusterConfig() {
        final Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "invalid-strategy");

        assertThatThrownBy(
                        () ->
                                RestartBackoffTimeStrategyFactoryLoader
                                        .createRestartBackoffTimeStrategyFactory(
                                                DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
                                                new Configuration(),
                                                conf,
                                                false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNoStrategySpecifiedWhenCheckpointingEnabled() {
        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
                        new Configuration(),
                        new Configuration(),
                        true);

        RestartBackoffTimeStrategy strategy = factory.create();
        assertThat(strategy).isInstanceOf(ExponentialDelayRestartBackoffTimeStrategy.class);

        ExponentialDelayRestartBackoffTimeStrategy exponentialDelayStrategy =
                (ExponentialDelayRestartBackoffTimeStrategy) strategy;

        assertThat(exponentialDelayStrategy.getInitialBackoffMS())
                .isEqualTo(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                                .defaultValue()
                                .toMillis());
        assertThat(exponentialDelayStrategy.getMaxBackoffMS())
                .isEqualTo(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF
                                .defaultValue()
                                .toMillis());
        assertThat(exponentialDelayStrategy.getBackoffMultiplier())
                .isEqualTo(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER
                                .defaultValue());
        assertThat(exponentialDelayStrategy.getResetBackoffThresholdMS())
                .isEqualTo(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD
                                .defaultValue()
                                .toMillis());
        assertThat(exponentialDelayStrategy.getJitterFactor())
                .isEqualTo(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR
                                .defaultValue());
        assertThat(exponentialDelayStrategy.getAttemptsBeforeResetBackoff())
                .isEqualTo(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS
                                .defaultValue());
    }

    @Test
    void testNoStrategySpecifiedWhenCheckpointingDisabled() {
        final RestartBackoffTimeStrategy.Factory factory =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                        DEFAULT_JOB_LEVEL_RESTART_CONFIGURATION,
                        new Configuration(),
                        new Configuration(),
                        false);

        assertThat(factory)
                .isInstanceOf(
                        NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.class);
    }
}
