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

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RestartStrategies}. */
class RestartStrategyTest {

    /**
     * Tests that in a streaming use case where checkpointing is enabled, there is no default
     * strategy set on the client side.
     */
    @Test
    void testFallbackStrategyOnClientSideWhenCheckpointingEnabled() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

        env.fromData(1).print();

        StreamGraph graph = env.getStreamGraph();
        JobGraph jobGraph = graph.getJobGraph();

        RestartStrategies.RestartStrategyConfiguration restartStrategy =
                jobGraph.getSerializedExecutionConfig()
                        .deserializeValue(getClass().getClassLoader())
                        .getRestartStrategy();

        assertThat(restartStrategy)
                .isNotNull()
                .isInstanceOf(RestartStrategies.FallbackRestartStrategyConfiguration.class);
    }

    /**
     * Checks that in a streaming use case where checkpointing is enabled and the number of
     * execution retries is set to 0, restarting is deactivated.
     */
    @Test
    void testNoRestartingWhenCheckpointingAndExplicitExecutionRetriesZero() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setNumberOfExecutionRetries(0);

        env.fromData(1).print();

        StreamGraph graph = env.getStreamGraph();
        JobGraph jobGraph = graph.getJobGraph();

        RestartStrategies.RestartStrategyConfiguration restartStrategy =
                jobGraph.getSerializedExecutionConfig()
                        .deserializeValue(getClass().getClassLoader())
                        .getRestartStrategy();

        assertThat(restartStrategy)
                .isNotNull()
                .isInstanceOf(RestartStrategies.NoRestartStrategyConfiguration.class);
    }

    /**
     * Checks that in a streaming use case where checkpointing is enabled and the number of
     * execution retries is set to 42 and the delay to 1337, fixed delay restarting is used.
     */
    @Test
    void testFixedRestartingWhenCheckpointingAndExplicitExecutionRetriesNonZero() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setNumberOfExecutionRetries(42);
        env.getConfig().setExecutionRetryDelay(1337);

        env.fromData(1).print();

        StreamGraph graph = env.getStreamGraph();
        JobGraph jobGraph = graph.getJobGraph();

        RestartStrategies.RestartStrategyConfiguration restartStrategy =
                jobGraph.getSerializedExecutionConfig()
                        .deserializeValue(getClass().getClassLoader())
                        .getRestartStrategy();

        assertThat(restartStrategy)
                .isNotNull()
                .isInstanceOfSatisfying(
                        RestartStrategies.FixedDelayRestartStrategyConfiguration.class,
                        strategy ->
                                assertThat(strategy)
                                        .satisfies(
                                                fixedDelayRestartStrategy -> {
                                                    assertThat(
                                                                    fixedDelayRestartStrategy
                                                                            .getRestartAttempts())
                                                            .isEqualTo(42);
                                                    assertThat(
                                                                    fixedDelayRestartStrategy
                                                                            .getDurationBetweenAttempts()
                                                                            .toMillis())
                                                            .isEqualTo(1337);
                                                }));
    }
}
