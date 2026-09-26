/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.executiongraph.failover.ExponentialDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailureRateRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerConstants.EG_CREATION_RETRY_CONFIG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionGraphCreationRetryStrategyFactoryLoader}. */
class ExecutionGraphCreationRetryStrategyFactoryLoaderTest {

    private static final String STRATEGY_KEY =
            EG_CREATION_RETRY_CONFIG_PREFIX + RestartStrategyOptions.RESTART_STRATEGY.key();

    @Test
    void testDefaultsToNoRestartWhenUnset() {
        assertThat(create(new Configuration(), new Configuration()))
                .isInstanceOf(NoRestartBackoffTimeStrategy.class);
    }

    @Test
    void testExplicitNoneDisablesRetry() {
        final Configuration jobConfig = new Configuration();
        jobConfig.setString(STRATEGY_KEY, "none");

        assertThat(create(jobConfig, new Configuration()))
                .isInstanceOf(NoRestartBackoffTimeStrategy.class);
    }

    @Test
    void testFixedDelayStrategyFromJobConfig() {
        final Configuration jobConfig = new Configuration();
        jobConfig.setString(STRATEGY_KEY, "fixed-delay");

        assertThat(create(jobConfig, new Configuration()))
                .isInstanceOf(FixedDelayRestartBackoffTimeStrategy.class);
    }

    @Test
    void testFailureRateStrategyFromClusterConfig() {
        final Configuration clusterConfig = new Configuration();
        clusterConfig.setString(STRATEGY_KEY, "failure-rate");

        assertThat(create(new Configuration(), clusterConfig))
                .isInstanceOf(FailureRateRestartBackoffTimeStrategy.class);
    }

    @Test
    void testExponentialDelayStrategyFromClusterConfig() {
        final Configuration clusterConfig = new Configuration();
        clusterConfig.setString(STRATEGY_KEY, "exponential-delay");

        assertThat(create(new Configuration(), clusterConfig))
                .isInstanceOf(ExponentialDelayRestartBackoffTimeStrategy.class);
    }

    /**
     * The per-type parameters must be read from under the dedicated prefix, not from the plain
     * {@code restart-strategy.*} namespace. This is what regressed when the loader copied the
     * prefixed configuration through {@code Configuration#addAll}, which silently dropped every key.
     */
    @Test
    void testPrefixedPerTypeParametersAreApplied() {
        final Configuration jobConfig = new Configuration();
        jobConfig.setString(STRATEGY_KEY, "fixed-delay");
        jobConfig.setString(
                EG_CREATION_RETRY_CONFIG_PREFIX
                        + RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY.key(),
                "7 s");

        final RestartBackoffTimeStrategy strategy = create(jobConfig, new Configuration());

        assertThat(strategy).isInstanceOf(FixedDelayRestartBackoffTimeStrategy.class);
        assertThat(strategy.getBackoffTime()).isEqualTo(Duration.ofSeconds(7).toMillis());
    }

    @Test
    void testJobConfigTakesPrecedenceOverClusterConfig() {
        final Configuration jobConfig = new Configuration();
        jobConfig.setString(STRATEGY_KEY, "fixed-delay");

        final Configuration clusterConfig = new Configuration();
        clusterConfig.setString(STRATEGY_KEY, "exponential-delay");

        assertThat(create(jobConfig, clusterConfig))
                .isInstanceOf(FixedDelayRestartBackoffTimeStrategy.class);
    }

    @Test
    void testPrefixedRestartStrategyIsIndependentOfJobRestartStrategy() {
        // Only the job's runtime restart-strategy is configured; the EG-creation retry namespace is
        // left untouched, so EG-creation failures must not be retried.
        final Configuration clusterConfig = new Configuration();
        clusterConfig.setString(RestartStrategyOptions.RESTART_STRATEGY.key(), "exponential-delay");

        assertThat(create(new Configuration(), clusterConfig))
                .isInstanceOf(NoRestartBackoffTimeStrategy.class);
    }

    @Test
    void testUnknownStrategyThrows() {
        final Configuration jobConfig = new Configuration();
        jobConfig.setString(STRATEGY_KEY, "not-a-strategy");

        assertThatThrownBy(() -> create(jobConfig, new Configuration()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RestartBackoffTimeStrategy create(
            Configuration jobConfig, Configuration clusterConfig) {
        return ExecutionGraphCreationRetryStrategyFactoryLoader.createFactory(
                        jobConfig, clusterConfig)
                .create();
    }
}
