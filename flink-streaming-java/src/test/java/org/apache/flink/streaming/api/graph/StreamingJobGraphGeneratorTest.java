/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamingJobGraphGenerator}. */
@SuppressWarnings("serial")
class StreamingJobGraphGeneratorTest extends JobGraphGeneratorTestBase {
    @Override
    JobGraph createJobGraph(StreamGraph streamGraph) {
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    @Override
    void verifyManagedMemoryFractionForUnknownResourceSpec(
            JobVertex vertex1,
            JobVertex vertex2,
            JobVertex vertex3,
            Configuration taskManagerConfig) {
        final StreamConfig sourceConfig = new StreamConfig(vertex1.getConfiguration());
        verifyFractions(sourceConfig, 0.6 / 2, 0.0, 0.0, taskManagerConfig);

        final StreamConfig map1Config =
                Iterables.getOnlyElement(
                        sourceConfig
                                .getTransitiveChainedTaskConfigs(
                                        JobGraphGeneratorTestBase.class.getClassLoader())
                                .values());
        verifyFractions(map1Config, 0.6 / 2, 0.4, 0.0, taskManagerConfig);

        final StreamConfig map2Config = new StreamConfig(vertex2.getConfiguration());
        verifyFractions(map2Config, 0.0, 0.4, 0.0, taskManagerConfig);

        final StreamConfig map3Config = new StreamConfig(vertex3.getConfiguration());
        verifyFractions(map3Config, 1.0, 0.0, 0.0, taskManagerConfig);
    }

    /**
     * Verifies that each {@link NonChainedOutput} emitted for a source with multiple downstream
     * consumers carries the {@link JobVertexID} of the actual downstream {@link JobVertex} it
     * feeds. This target id is what the runtime uses to key the per-downstream-target {@code
     * numRecordsOut} metric registered by {@code TaskIOMetricGroup#reuseRecordsOutputCounter}.
     */
    @Test
    void testNonChainedOutputCarriesDownstreamJobVertexId() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Integer> source = env.fromData(1, 2, 3);
        source.rebalance().map(x -> x).name("mapA").sinkTo(new DiscardingSink<>()).name("sinkA");
        source.rebalance().map(x -> x).name("mapB").sinkTo(new DiscardingSink<>()).name("sinkB");

        final JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        final JobVertex sourceVertex = vertices.get(0);
        final JobVertex mapAVertex = findJobVertex(vertices, "mapA");
        final JobVertex mapBVertex = findJobVertex(vertices, "mapB");

        final StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        final List<NonChainedOutput> nonChainedOutputs =
                sourceConfig.getVertexNonChainedOutputs(getClass().getClassLoader());
        assertThat(nonChainedOutputs).hasSize(2);
        assertThat(nonChainedOutputs)
                .extracting(NonChainedOutput::getTargetNodeId)
                .containsExactlyInAnyOrder(mapAVertex.getID(), mapBVertex.getID());
    }

    /**
     * Broadcast fan-out variant of {@link #testNonChainedOutputCarriesDownstreamJobVertexId()}:
     * distinct downstream vertices must each receive their own {@link NonChainedOutput} carrying
     * the matching {@link JobVertexID}. Ensures the planner does not collapse broadcast outputs
     * into a single target id, which would break the per-target metric.
     */
    @Test
    void testBroadcastFanOutTargetJobVertexIds() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Integer> source = env.fromData(1, 2, 3);
        final DataStream<Integer> broadcast = source.broadcast();
        broadcast.map(x -> x).name("mapA").disableChaining().sinkTo(new DiscardingSink<>());
        broadcast.map(x -> x).name("mapB").disableChaining().sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = createJobGraph(env.getStreamGraph());
        final List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        final JobVertex sourceVertex = vertices.get(0);
        final JobVertex mapAVertex = findJobVertex(vertices, "mapA");
        final JobVertex mapBVertex = findJobVertex(vertices, "mapB");

        final StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        final List<NonChainedOutput> nonChainedOutputs =
                sourceConfig.getVertexNonChainedOutputs(getClass().getClassLoader());
        assertThat(nonChainedOutputs).hasSize(2);
        assertThat(nonChainedOutputs)
                .extracting(NonChainedOutput::getTargetNodeId)
                .containsExactlyInAnyOrder(mapAVertex.getID(), mapBVertex.getID())
                .doesNotContainNull();
    }

    private static JobVertex findJobVertex(List<JobVertex> vertices, String name) {
        for (JobVertex jobVertex : vertices) {
            if (jobVertex.getName().contains(name)) {
                return jobVertex;
            }
        }
        throw new AssertionError("No job vertex found with name containing: " + name);
    }
}
