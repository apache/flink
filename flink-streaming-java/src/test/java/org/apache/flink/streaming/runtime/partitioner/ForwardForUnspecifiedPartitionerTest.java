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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ForwardForUnspecifiedPartitioner}. */
class ForwardForUnspecifiedPartitionerTest {

    @Test
    void testConvertToForwardPartitioner() {
        JobGraph jobGraph =
                StreamPartitionerTestUtils.createJobGraph(
                        "group1", "group1", new ForwardForUnspecifiedPartitioner<>());
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size()).isEqualTo(1);
        JobVertex vertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(vertex.getConfiguration());
        StreamEdge edge = sourceConfig.getChainedOutputs(getClass().getClassLoader()).get(0);
        assertThat(edge.getPartitioner()).isInstanceOf(ForwardPartitioner.class);
    }

    @Test
    void testConvertToRescalePartitioner() {
        JobGraph jobGraph =
                StreamPartitionerTestUtils.createJobGraph(
                        "group1", "group2", new ForwardForUnspecifiedPartitioner<>());
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size()).isEqualTo(2);
        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        NonChainedOutput output =
                sourceConfig.getOperatorNonChainedOutputs(getClass().getClassLoader()).get(0);
        assertThat(output.getPartitioner()).isInstanceOf(RescalePartitioner.class);
    }

    @Test
    void testConvertToCorrectPartitioner() {
        testConvertToCorrectPartitioner(null, RescalePartitioner.class);
        testConvertToCorrectPartitioner(
                JobManagerOptions.SchedulerType.AdaptiveBatch, RescalePartitioner.class);
        testConvertToCorrectPartitioner(
                JobManagerOptions.SchedulerType.Default, ForwardPartitioner.class);
    }

    private void testConvertToCorrectPartitioner(
            JobManagerOptions.SchedulerType scheduler, Class<?> expectedPartitioner) {
        Configuration configuration = new Configuration();
        if (scheduler != null) {
            configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        }
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.fromSequence(0, 99)
                .slotSharingGroup("group1")
                .name("source")
                .sinkTo(new DiscardingSink<>())
                .slotSharingGroup("group2")
                .name("sink");

        List<JobVertex> jobVertices =
                env.getStreamGraph().getJobGraph().getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size()).isEqualTo(2);

        JobVertex sourceVertex = jobVertices.get(0);
        StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        NonChainedOutput output =
                sourceConfig.getOperatorNonChainedOutputs(getClass().getClassLoader()).get(0);
        assertThat(output.getPartitioner()).isInstanceOf(expectedPartitioner);
    }
}
