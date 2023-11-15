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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.delegation.Executor;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultExecutor}. */
public class DefaultExecutorTest {

    @Test
    public void testJobName() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Executor executor = new DefaultExecutor(env);
        final List<Transformation<?>> dummyTransformations =
                Collections.singletonList(
                        env.fromElements(1, 2, 3)
                                .sinkTo(new DiscardingSink<>())
                                .getTransformation());

        final Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.NAME, "Custom Name");

        // default
        testJobName(
                executor.createPipeline(dummyTransformations, new Configuration(), "Default Name"),
                "Default Name");

        // Table API specific
        testJobName(
                executor.createPipeline(dummyTransformations, configuration, "Default Name"),
                "Custom Name");

        // DataStream API specific
        env.configure(configuration);
        testJobName(
                executor.createPipeline(dummyTransformations, new Configuration(), "Default Name"),
                "Custom Name");
    }

    @Test
    public void testDefaultBatchProperties() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Executor executor = new DefaultExecutor(env);

        final List<Transformation<?>> dummyTransformations =
                Collections.singletonList(
                        env.fromElements(1, 2, 3)
                                .sinkTo(new DiscardingSink<>())
                                .getTransformation());

        final Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        final StreamGraph streamGraph =
                (StreamGraph)
                        executor.createPipeline(
                                dummyTransformations, configuration, "Default Name");

        assertThat(streamGraph.getExecutionConfig().isObjectReuseEnabled()).isTrue();
        assertThat(streamGraph.getExecutionConfig().getLatencyTrackingInterval()).isEqualTo(0);
        assertThat(streamGraph.isChainingEnabled()).isTrue();
        assertThat(streamGraph.isAllVerticesInSameSlotSharingGroupByDefault()).isFalse();
        assertThat(streamGraph.getCheckpointConfig().isCheckpointingEnabled()).isFalse();
        assertThat(streamGraph.getGlobalStreamExchangeMode())
                .isEqualTo(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
    }

    private void testJobName(Pipeline pipeline, String expectedJobName) {
        assertThat(((StreamGraph) pipeline).getJobName()).isEqualTo(expectedJobName);
    }
}
