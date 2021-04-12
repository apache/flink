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
import org.apache.flink.api.common.restartstrategy.RestartStrategies.ExponentialDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FallbackRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.NoRestartStrategyConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.time.Duration;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A utility class to load {@link RestartBackoffTimeStrategy.Factory} from the configuration. */
public final class RestartBackoffTimeStrategyFactoryLoader {

    static final int DEFAULT_RESTART_ATTEMPTS = Integer.MAX_VALUE;

    static final long DEFAULT_RESTART_DELAY = Duration.ofSeconds(1L).toMillis();

    private RestartBackoffTimeStrategyFactoryLoader() {}

    /**
     * Creates {@link RestartBackoffTimeStrategy.Factory} from the given configuration.
     *
     * <p>The strategy factory is decided in order as follows:
     *
     * <ol>
     *   <li>Strategy set within job graph, i.e. {@link
     *       RestartStrategies.RestartStrategyConfiguration}, unless the config is {@link
     *       RestartStrategies.FallbackRestartStrategyConfiguration}.
     *   <li>Strategy set in the cluster(server-side) config (flink-conf.yaml), unless the strategy
     *       is not specified
     *   <li>{@link
     *       FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory} if
     *       checkpointing is enabled. Otherwise {@link
     *       NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory}
     * </ol>
     *
     * @param jobRestartStrategyConfiguration restart configuration given within the job graph
     * @param clusterConfiguration cluster(server-side) configuration
     * @param isCheckpointingEnabled if checkpointing is enabled for the job
     * @return new version restart strategy factory
     */
    public static RestartBackoffTimeStrategy.Factory createRestartBackoffTimeStrategyFactory(
            final RestartStrategies.RestartStrategyConfiguration jobRestartStrategyConfiguration,
            final Configuration clusterConfiguration,
            final boolean isCheckpointingEnabled) {

        checkNotNull(jobRestartStrategyConfiguration);
        checkNotNull(clusterConfiguration);

        return getJobRestartStrategyFactory(jobRestartStrategyConfiguration)
                .orElse(
                        getClusterRestartStrategyFactory(clusterConfiguration)
                                .orElse(getDefaultRestartStrategyFactory(isCheckpointingEnabled)));
    }

    private static Optional<RestartBackoffTimeStrategy.Factory> getJobRestartStrategyFactory(
            final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {

        if (restartStrategyConfiguration instanceof NoRestartStrategyConfiguration) {
            return Optional.of(
                    NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE);
        } else if (restartStrategyConfiguration instanceof FixedDelayRestartStrategyConfiguration) {
            final FixedDelayRestartStrategyConfiguration fixedDelayConfig =
                    (FixedDelayRestartStrategyConfiguration) restartStrategyConfiguration;

            return Optional.of(
                    new FixedDelayRestartBackoffTimeStrategy
                            .FixedDelayRestartBackoffTimeStrategyFactory(
                            fixedDelayConfig.getRestartAttempts(),
                            fixedDelayConfig.getDelayBetweenAttemptsInterval().toMilliseconds()));
        } else if (restartStrategyConfiguration
                instanceof FailureRateRestartStrategyConfiguration) {
            final FailureRateRestartStrategyConfiguration failureRateConfig =
                    (FailureRateRestartStrategyConfiguration) restartStrategyConfiguration;

            return Optional.of(
                    new FailureRateRestartBackoffTimeStrategy
                            .FailureRateRestartBackoffTimeStrategyFactory(
                            failureRateConfig.getMaxFailureRate(),
                            failureRateConfig.getFailureInterval().toMilliseconds(),
                            failureRateConfig.getDelayBetweenAttemptsInterval().toMilliseconds()));
        } else if (restartStrategyConfiguration instanceof FallbackRestartStrategyConfiguration) {
            return Optional.empty();
        } else if (restartStrategyConfiguration
                instanceof ExponentialDelayRestartStrategyConfiguration) {
            final ExponentialDelayRestartStrategyConfiguration exponentialDelayConfig =
                    (ExponentialDelayRestartStrategyConfiguration) restartStrategyConfiguration;
            return Optional.of(
                    new ExponentialDelayRestartBackoffTimeStrategy
                            .ExponentialDelayRestartBackoffTimeStrategyFactory(
                            exponentialDelayConfig.getInitialBackoff().toMilliseconds(),
                            exponentialDelayConfig.getMaxBackoff().toMilliseconds(),
                            exponentialDelayConfig.getBackoffMultiplier(),
                            exponentialDelayConfig.getResetBackoffThreshold().toMilliseconds(),
                            exponentialDelayConfig.getJitterFactor()));
        } else {
            throw new IllegalArgumentException(
                    "Unknown restart strategy configuration " + restartStrategyConfiguration + ".");
        }
    }

    private static Optional<RestartBackoffTimeStrategy.Factory> getClusterRestartStrategyFactory(
            final Configuration clusterConfiguration) {

        final String restartStrategyName =
                clusterConfiguration.getString(RestartStrategyOptions.RESTART_STRATEGY);
        if (restartStrategyName == null) {
            return Optional.empty();
        }

        switch (restartStrategyName.toLowerCase()) {
            case "none":
            case "off":
            case "disable":
                return Optional.of(
                        NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE);
            case "fixeddelay":
            case "fixed-delay":
                return Optional.of(
                        FixedDelayRestartBackoffTimeStrategy.createFactory(clusterConfiguration));
            case "failurerate":
            case "failure-rate":
                return Optional.of(
                        FailureRateRestartBackoffTimeStrategy.createFactory(clusterConfiguration));
            case "exponentialdelay":
            case "exponential-delay":
                return Optional.of(
                        ExponentialDelayRestartBackoffTimeStrategy.createFactory(
                                clusterConfiguration));
            default:
                throw new IllegalArgumentException(
                        "Unknown restart strategy " + restartStrategyName + ".");
        }
    }

    private static RestartBackoffTimeStrategy.Factory getDefaultRestartStrategyFactory(
            final boolean isCheckpointingEnabled) {

        if (isCheckpointingEnabled) {
            // fixed delay restart strategy with default params
            return new FixedDelayRestartBackoffTimeStrategy
                    .FixedDelayRestartBackoffTimeStrategyFactory(
                    DEFAULT_RESTART_ATTEMPTS, DEFAULT_RESTART_DELAY);
        } else {
            return NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE;
        }
    }
}
