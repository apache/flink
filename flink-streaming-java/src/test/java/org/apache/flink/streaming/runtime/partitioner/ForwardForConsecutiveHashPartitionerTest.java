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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test for {@link ForwardForConsecutiveHashPartitioner}. */
class ForwardForConsecutiveHashPartitionerTest {

    @Test
    void testConvertToForwardPartitioner() {
        testConvertToForwardPartitioner(StreamExchangeMode.BATCH);
        testConvertToForwardPartitioner(StreamExchangeMode.PIPELINED);
        testConvertToForwardPartitioner(StreamExchangeMode.UNDEFINED);
    }

    private void testConvertToForwardPartitioner(StreamExchangeMode streamExchangeMode) {
        JobGraph jobGraph =
                StreamPartitionerTestUtils.createJobGraph(
                        "group1",
                        "group1",
                        new ForwardForConsecutiveHashPartitioner<>(
                                new KeyGroupStreamPartitioner<>(record -> 0L, 100)),
                        streamExchangeMode);
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        Assertions.assertThat(jobVertices.size()).isEqualTo(1);
        JobVertex vertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(vertex.getConfiguration());
        StreamEdge edge = sourceConfig.getChainedOutputs(getClass().getClassLoader()).get(0);
        Assertions.assertThat(edge.getPartitioner()).isInstanceOf(ForwardPartitioner.class);
    }

    @Test
    void testConvertToHashPartitioner() {
        testConvertToHashPartitioner(StreamExchangeMode.BATCH);
        testConvertToHashPartitioner(StreamExchangeMode.PIPELINED);
        testConvertToHashPartitioner(StreamExchangeMode.UNDEFINED);
    }

    private void testConvertToHashPartitioner(StreamExchangeMode streamExchangeMode) {
        JobGraph jobGraph =
                StreamPartitionerTestUtils.createJobGraph(
                        "group1",
                        "group2",
                        new ForwardForConsecutiveHashPartitioner<>(
                                new KeyGroupStreamPartitioner<>(record -> 0L, 100)),
                        streamExchangeMode);
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        Assertions.assertThat(jobVertices.size()).isEqualTo(2);
        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        NonChainedOutput output =
                sourceConfig.getOperatorNonChainedOutputs(getClass().getClassLoader()).get(0);
        Assertions.assertThat(output.getPartitioner())
                .isInstanceOf(KeyGroupStreamPartitioner.class);
    }
}
