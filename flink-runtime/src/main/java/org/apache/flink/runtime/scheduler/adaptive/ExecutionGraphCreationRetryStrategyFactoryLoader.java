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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.executiongraph.failover.ExponentialDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailureRateRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;

import java.util.Optional;

/**
 * Loads the dedicated {@link RestartBackoffTimeStrategy} used to retry ExecutionGraph-creation
 * failures, independent of the job's {@code restart-strategy} so these retries do not consume the
 * job's runtime restart budget.
 */
final class ExecutionGraphCreationRetryStrategyFactoryLoader {

    private ExecutionGraphCreationRetryStrategyFactoryLoader() {}

    /**
     * Builds the factory from the {@code
     * jobmanager.adaptive-scheduler.retry-execution-graph-creation.} config namespace: the strategy
     * type is taken from the job config, then the cluster config; if neither configures one,
     * ExecutionGraph-creation failures are not retried (no-restart). Reuses only the public
     * per-type {@code createFactory} methods, never the base loader's checkpointing-conditional
     * default.
     */
    static RestartBackoffTimeStrategy.Factory createFactory(
            Configuration jobConfiguration, Configuration clusterConfiguration) {
        return factoryFromConfig(prefixed(jobConfiguration))
                .orElseGet(
                        () ->
                                factoryFromConfig(prefixed(clusterConfiguration))
                                        .orElse(
                                                NoRestartBackoffTimeStrategy
                                                        .NoRestartBackoffTimeStrategyFactory
                                                        .INSTANCE));
    }

    private static Optional<RestartBackoffTimeStrategy.Factory> factoryFromConfig(
            Configuration configuration) {
        return configuration
                .getOptional(RestartStrategyOptions.RESTART_STRATEGY)
                .map(
                        strategyName -> {
                            switch (RestartStrategyOptions.RestartStrategyType.of(
                                    strategyName.toLowerCase())) {
                                case NO_RESTART_STRATEGY:
                                    return NoRestartBackoffTimeStrategy
                                            .NoRestartBackoffTimeStrategyFactory.INSTANCE;
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
                                            "Unknown restart strategy " + strategyName + ".");
                            }
                        });
    }

    private static Configuration prefixed(Configuration configuration) {
        return new DelegatingConfiguration(
                configuration, AdaptiveSchedulerConstants.EG_CREATION_RETRY_CONFIG_PREFIX);
    }
}
