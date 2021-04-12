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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A utility class to load NG failover strategy factories from the configuration. */
public final class FailoverStrategyFactoryLoader {

    /** Config name for the {@link RestartAllFailoverStrategy}. */
    public static final String FULL_RESTART_STRATEGY_NAME = "full";

    /** Config name for the {@link RestartPipelinedRegionFailoverStrategy}. */
    public static final String PIPELINED_REGION_RESTART_STRATEGY_NAME = "region";

    private FailoverStrategyFactoryLoader() {}

    /**
     * Loads a {@link FailoverStrategy.Factory} from the given configuration.
     *
     * @param config which specifies the failover strategy factory to load
     * @return failover strategy factory loaded
     */
    public static FailoverStrategy.Factory loadFailoverStrategyFactory(final Configuration config) {
        checkNotNull(config);

        final String strategyParam =
                config.getString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY);

        switch (strategyParam.toLowerCase()) {
            case FULL_RESTART_STRATEGY_NAME:
                return new RestartAllFailoverStrategy.Factory();

            case PIPELINED_REGION_RESTART_STRATEGY_NAME:
                return new RestartPipelinedRegionFailoverStrategy.Factory();

            default:
                throw new IllegalConfigurationException(
                        "Unknown failover strategy: " + strategyParam);
        }
    }
}
