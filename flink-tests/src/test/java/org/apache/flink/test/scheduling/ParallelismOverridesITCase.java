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

package org.apache.flink.test.scheduling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PipelineOptions#PARALLELISM_OVERRIDES} through {@code env.execute()}, i.e. the StreamGraph
 * submission every client performs, for the streaming schedulers: from the cluster configuration,
 * from the job configuration, and with both set.
 */
@ExtendWith(ParameterizedTestExtension.class)
class ParallelismOverridesITCase {

    private static final String SOURCE_UID = "override-source";
    private static final String MAP_UID = "override-map";

    private static final JobVertexID MAP_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(MAP_UID);

    @Parameter public SchedulerType scheduler;

    private MiniCluster miniCluster;

    @Parameters(name = "scheduler={0}")
    private static Collection<SchedulerType> schedulers() {
        return Arrays.asList(SchedulerType.Default, SchedulerType.Adaptive);
    }

    @AfterEach
    void tearDown() throws Exception {
        TestStreamEnvironment.unsetAsContext();
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @TestTemplate
    void testOverrideFromClusterConfiguration() throws Exception {
        startCluster(Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "3"));

        final ArchivedExecutionGraph graph = runSourceMapSink(new Configuration());

        assertThat(graph.getAllVertices().get(MAP_VERTEX_ID).getParallelism()).isEqualTo(3);
    }

    @TestTemplate
    void testOverrideFromJobConfiguration() throws Exception {
        startCluster(Collections.emptyMap());
        final Configuration jobConfiguration = new Configuration();
        jobConfiguration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "3"));

        final ArchivedExecutionGraph graph = runSourceMapSink(jobConfiguration);

        assertThat(graph.getAllVertices().get(MAP_VERTEX_ID).getParallelism()).isEqualTo(3);
    }

    @TestTemplate
    void testJobConfigurationWinsOverClusterConfiguration() throws Exception {
        startCluster(Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "2"));
        final Configuration jobConfiguration = new Configuration();
        jobConfiguration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "3"));

        final ArchivedExecutionGraph graph = runSourceMapSink(jobConfiguration);

        assertThat(graph.getAllVertices().get(MAP_VERTEX_ID).getParallelism()).isEqualTo(3);
    }

    private ArchivedExecutionGraph runSourceMapSink(Configuration jobConfiguration)
            throws Exception {
        final StreamExecutionEnvironment env = environment(jobConfiguration);
        env.fromSequence(0, 999)
                .setParallelism(1)
                .uid(SOURCE_UID)
                .name("source")
                .map(v -> v + 1)
                .uid(MAP_UID)
                .name("map")
                .sinkTo(new DiscardingSink<>());
        return run(env);
    }

    private ArchivedExecutionGraph run(StreamExecutionEnvironment env) throws Exception {
        final JobClient jobClient = env.executeAsync();
        jobClient.getJobExecutionResult().get();
        return miniCluster.getArchivedExecutionGraph(jobClient.getJobID()).get();
    }

    private StreamExecutionEnvironment environment(Configuration jobConfiguration) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(jobConfiguration);
        env.setParallelism(1);
        env.disableOperatorChaining();
        return env;
    }

    private void startCluster(Map<String, String> overrides) throws Exception {
        miniCluster = ParallelismOverridesTestUtils.startMiniCluster(scheduler, overrides);
        TestStreamEnvironment.setAsContext(miniCluster, 1);
    }
}
