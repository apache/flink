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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link ForwardForConsecutiveHashPartitioner}. */
public class ForwardForConsecutiveHashPartitionerTest extends TestLogger {

    @Test
    public void testConvertToForwardPartitioner() {
        JobGraph jobGraph = createJobGraph("group1", "group1");
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size(), is(1));
        JobVertex vertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(vertex.getConfiguration());
        StreamEdge edge = sourceConfig.getChainedOutputs(getClass().getClassLoader()).get(0);
        assertThat(edge.getPartitioner(), instanceOf(ForwardPartitioner.class));
    }

    @Test
    public void testConvertToHashPartitioner() {
        JobGraph jobGraph = createJobGraph("group1", "group2");
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        assertThat(jobVertices.size(), is(2));
        JobVertex sourceVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);

        StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
        StreamEdge edge = sourceConfig.getNonChainedOutputs(getClass().getClassLoader()).get(0);
        assertThat(edge.getPartitioner(), instanceOf(KeyGroupStreamPartitioner.class));
    }

    private JobGraph createJobGraph(String sourceSlotSharingGroup, String sinkSlotSharingGroup) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().setDynamicGraph(true);

        final DataStream<Long> source =
                env.fromSequence(0, 99).slotSharingGroup(sourceSlotSharingGroup).name("source");

        setForwardForConsecutiveHashPartitioner(
                        source,
                        new ForwardForConsecutiveHashPartitioner<>(
                                new KeyGroupStreamPartitioner<>(record -> 0L, 100)))
                .addSink(new DiscardingSink<>())
                .slotSharingGroup(sinkSlotSharingGroup)
                .name("sink");

        return env.getStreamGraph().getJobGraph();
    }

    private <T> DataStream<T> setForwardForConsecutiveHashPartitioner(
            DataStream<T> dataStream,
            ForwardForConsecutiveHashPartitioner<T> forwardForConsecutiveHashPartitioner) {
        return new DataStream<T>(
                dataStream.getExecutionEnvironment(),
                new PartitionTransformation<T>(
                        dataStream.getTransformation(), forwardForConsecutiveHashPartitioner));
    }
}
