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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/** Tests for {@link FailoverStrategyFactoryLoader}. */
public class FailoverStrategyFactoryLoaderTest extends TestLogger {

    @Test
    public void testLoadRestartAllStrategyFactory() {
        final Configuration config = new Configuration();
        config.setString(
                JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
                FailoverStrategyFactoryLoader.FULL_RESTART_STRATEGY_NAME);
        assertThat(
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config),
                instanceOf(RestartAllFailoverStrategy.Factory.class));
    }

    @Test
    public void testLoadRestartPipelinedRegionStrategyFactory() {
        final Configuration config = new Configuration();
        config.setString(
                JobManagerOptions.EXECUTION_FAILOVER_STRATEGY,
                FailoverStrategyFactoryLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME);
        assertThat(
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config),
                instanceOf(RestartPipelinedRegionFailoverStrategy.Factory.class));
    }

    @Test
    public void testDefaultFailoverStrategyIsRegion() {
        final Configuration config = new Configuration();
        assertThat(
                FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config),
                instanceOf(RestartPipelinedRegionFailoverStrategy.Factory.class));
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testLoadFromInvalidConfiguration() {
        final Configuration config = new Configuration();
        config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "invalidStrategy");
        FailoverStrategyFactoryLoader.loadFailoverStrategyFactory(config);
    }
}
