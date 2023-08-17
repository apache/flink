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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailoverStrategyFactoryLoader}. */
class FailoverStrategyFactoryLoaderTest {

    @Test
    void testLoadRestartAllStrategyFactory() {
        final Configuration config = new Configuration();
        config.setString(
                JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
                FailoverStrategyFactoryLoader.FULL_RESTART_STRATEGY_NAME);
        assertThat(FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config))
                .isInstanceOf(RestartAllFailoverStrategy.Factory.class);
    }

    @Test
    void testLoadRestartPipelinedRegionStrategyFactory() {
        final Configuration config = new Configuration();
        config.setString(
                JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
                FailoverStrategyFactoryLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);
        assertThat(FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config))
                .isInstanceOf(RestartPipelinedRegionFailoverStrategy.Factory.class);
    }

    @Test
    void testDefaultFailoverStrategyIsRegion() {
        final Configuration config = new Configuration();
        assertThat(FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config))
                .isInstanceOf(RestartPipelinedRegionFailoverStrategy.Factory.class);
    }

    @Test
    void testLoadFromInvalidConfiguration() {
        final Configuration config = new Configuration();
        config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "invalidStrategy");
        assertThatThrownBy(() -> FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }
}
