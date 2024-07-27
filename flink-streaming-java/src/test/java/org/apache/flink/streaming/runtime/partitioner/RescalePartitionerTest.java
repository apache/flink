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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link RescalePartitioner}. */
@SuppressWarnings("serial")
class RescalePartitionerTest extends StreamPartitionerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Override
    StreamPartitioner<Tuple> createPartitioner() {
        StreamPartitioner<Tuple> partitioner = new RescalePartitioner<>();
        assertThat(partitioner.isBroadcast()).isFalse();
        return partitioner;
    }

    @Test
    void testSelectChannelsInterval() {
        streamPartitioner.setup(3);

        assertSelectedChannel(0);
        assertSelectedChannel(1);
        assertSelectedChannel(2);
        assertSelectedChannel(0);
    }

    @Test
    void testExecutionGraphGeneration() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        // get input data
        DataStream<String> text =
                env.addSource(
                                new ParallelSourceFunction<String>() {
                                    private static final long serialVersionUID =
                                            7772338606389180774L;

                                    @Override
                                    public void run(SourceContext<String> ctx) throws Exception {}

                                    @Override
                                    public void cancel() {}
                                })
                        .setParallelism(2);

        DataStream<Tuple2<String, Integer>> counts =
                text.rescale()
                        .flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                                    private static final long serialVersionUID =
                                            -5255930322161596829L;

                                    @Override
                                    public void flatMap(
                                            String value, Collector<Tuple2<String, Integer>> out)
                                            throws Exception {}
                                });

        counts.rescale().print().setParallelism(2);

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

        JobVertex sourceVertex = jobVertices.get(0);
        JobVertex mapVertex = jobVertices.get(1);
        JobVertex sinkVertex = jobVertices.get(2);

        assertThat(sourceVertex.getParallelism()).isEqualTo(2);
        assertThat(mapVertex.getParallelism()).isEqualTo(4);
        assertThat(sinkVertex.getParallelism()).isEqualTo(2);

        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(jobGraph))
                        .build(EXECUTOR_RESOURCE.getExecutor());

        try {
            eg.attachJobGraph(
                    jobVertices,
                    UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        } catch (JobException e) {
            fail("Building ExecutionGraph failed", e);
        }

        ExecutionJobVertex execSourceVertex = eg.getJobVertex(sourceVertex.getID());
        ExecutionJobVertex execMapVertex = eg.getJobVertex(mapVertex.getID());
        ExecutionJobVertex execSinkVertex = eg.getJobVertex(sinkVertex.getID());

        assertThat(execSourceVertex.getInputs()).isEmpty();
        assertThat(execMapVertex.getInputs()).hasSize(1);
        assertThat(execMapVertex.getParallelism()).isEqualTo(4);
        ExecutionVertex[] mapTaskVertices = execMapVertex.getTaskVertices();

        // verify that we have each parallel input partition exactly twice, i.e. that one source
        // sends to two unique mappers
        Map<Integer, Integer> mapInputPartitionCounts = new HashMap<>();
        for (ExecutionVertex mapTaskVertex : mapTaskVertices) {
            assertThat(mapTaskVertex.getNumberOfInputs()).isOne();
            assertThat(mapTaskVertex.getConsumedPartitionGroup(0)).hasSize(1);
            IntermediateResultPartitionID consumedPartitionId =
                    mapTaskVertex.getConsumedPartitionGroup(0).getFirst();
            assertThat(
                            mapTaskVertex
                                    .getExecutionGraphAccessor()
                                    .getResultPartitionOrThrow(consumedPartitionId)
                                    .getProducer()
                                    .getJobvertexId())
                    .isEqualTo(sourceVertex.getID());
            int inputPartition = consumedPartitionId.getPartitionNumber();
            if (!mapInputPartitionCounts.containsKey(inputPartition)) {
                mapInputPartitionCounts.put(inputPartition, 1);
            } else {
                mapInputPartitionCounts.put(
                        inputPartition, mapInputPartitionCounts.get(inputPartition) + 1);
            }
        }

        assertThat(mapInputPartitionCounts).hasSize(2);
        for (int count : mapInputPartitionCounts.values()) {
            assertThat(count).isEqualTo(2);
        }

        assertThat(execSinkVertex.getInputs()).hasSize(1);
        assertThat(execSinkVertex.getParallelism()).isEqualTo(2);
        ExecutionVertex[] sinkTaskVertices = execSinkVertex.getTaskVertices();
        InternalExecutionGraphAccessor executionGraphAccessor = execSinkVertex.getGraph();

        // verify each sink instance has two inputs from the map and that each map subpartition
        // only occurs in one unique input edge
        Set<Integer> mapSubpartitions = new HashSet<>();
        for (ExecutionVertex sinkTaskVertex : sinkTaskVertices) {
            assertThat(sinkTaskVertex.getNumberOfInputs()).isOne();
            assertThat(sinkTaskVertex.getConsumedPartitionGroup(0)).hasSize(2);
            for (IntermediateResultPartitionID consumedPartitionId :
                    sinkTaskVertex.getConsumedPartitionGroup(0)) {
                IntermediateResultPartition consumedPartition =
                        executionGraphAccessor.getResultPartitionOrThrow(consumedPartitionId);
                assertThat(consumedPartition.getProducer().getJobvertexId())
                        .isEqualTo(mapVertex.getID());
                int partitionNumber = consumedPartition.getPartitionNumber();
                assertThat(mapSubpartitions).doesNotContain(partitionNumber);
                mapSubpartitions.add(partitionNumber);
            }
        }

        assertThat(mapSubpartitions).hasSize(4);
    }
}
