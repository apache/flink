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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.util.Optional;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A utility class to load {@link RestartBackoffTimeStrategy.Factory} from the configuration. */
public final class RestartBackoffTimeStrategyFactoryLoader {

    private RestartBackoffTimeStrategyFactoryLoader() {}

    /**
     * Creates {@link RestartBackoffTimeStrategy.Factory} from the given configuration.
     *
     * <p>The strategy factory is decided in order as follows:
     *
     * <ol>
     *   <li>Strategy set in the job-level config
     *   <li>Strategy set in the cluster(server-side) config (config.yaml), unless the strategy is
     *       not specified
     *   <li>{@link
     *       FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory} if
     *       checkpointing is enabled. Otherwise {@link
     *       NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory}
     * </ol>
     *
     * @param jobConfiguration job configuration
     * @param clusterConfiguration cluster(server-side) configuration
     * @param isCheckpointingEnabled if checkpointing is enabled for the job
     * @return new version restart strategy factory
     */
    public static RestartBackoffTimeStrategy.Factory createRestartBackoffTimeStrategyFactory(
            final Configuration jobConfiguration,
            final Configuration clusterConfiguration,
            final boolean isCheckpointingEnabled) {

        checkNotNull(jobConfiguration);
        checkNotNull(clusterConfiguration);

        return getRestartStrategyFactoryFromConfig(jobConfiguration)
                .orElse(
                        getRestartStrategyFactoryFromConfig(clusterConfiguration)
                                .orElse(getDefaultRestartStrategyFactory(isCheckpointingEnabled)));
    }

    private static Optional<RestartBackoffTimeStrategy.Factory> getRestartStrategyFactoryFromConfig(
            final Configuration configuration) {
        final Optional<String> restartStrategyNameOptional =
                configuration.getOptional(RestartStrategyOptions.RESTART_STRATEGY);
        return restartStrategyNameOptional.map(
                restartStrategyName -> {
                    switch (RestartStrategyType.of(restartStrategyName.toLowerCase())) {
                        case NO_RESTART_STRATEGY:
                            return NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory
                                    .INSTANCE;
                        case FIXED_DELAY:
                            return FixedDelayRestartBackoffTimeStrategy.createFactory(
                                    configuration);
                        case FAILURE_RATE:
                            return FailureRateRestartBackoffTimeStrategy.createFactory(
                                    configuration);
                        case EXPONENTIAL_DELAY:
                            return ExponentialDelayRestartBackoffTimeStrategy.createFactory(
                                    configuration);
                        default:
                            throw new IllegalArgumentException(
                                    "Unknown restart strategy " + restartStrategyName + ".");
                    }
                });
    }

    private static RestartBackoffTimeStrategy.Factory getDefaultRestartStrategyFactory(
            final boolean isCheckpointingEnabled) {

        if (isCheckpointingEnabled) {
            // exponential delay restart strategy with default params
            return new ExponentialDelayRestartBackoffTimeStrategy
                    .ExponentialDelayRestartBackoffTimeStrategyFactory(
                    RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF
                            .defaultValue()
                            .toMillis(),
                    RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF
                            .defaultValue()
                            .toMillis(),
                    RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER
                            .defaultValue(),
                    RestartStrategyOptions
                            .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD
                            .defaultValue()
                            .toMillis(),
                    RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR
                            .defaultValue(),
                    RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS
                            .defaultValue());
        } else {
            return NoRestartBackoffTimeStrategy.NoRestartBackoffTimeStrategyFactory.INSTANCE;
        }
    }
}
