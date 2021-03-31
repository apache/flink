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
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/** Tests for {@link RescalePartitioner}. */
@SuppressWarnings("serial")
public class RescalePartitionerTest extends StreamPartitionerTest {

    @Override
    public StreamPartitioner<Tuple> createPartitioner() {
        StreamPartitioner<Tuple> partitioner = new RescalePartitioner<>();
        assertFalse(partitioner.isBroadcast());
        return partitioner;
    }

    @Test
    public void testSelectChannelsInterval() {
        streamPartitioner.setup(3);

        assertSelectedChannel(0);
        assertSelectedChannel(1);
        assertSelectedChannel(2);
        assertSelectedChannel(0);
    }

    @Test
    public void testExecutionGraphGeneration() throws Exception {
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

        assertEquals(2, sourceVertex.getParallelism());
        assertEquals(4, mapVertex.getParallelism());
        assertEquals(2, sinkVertex.getParallelism());

        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(jobGraph))
                        .build();

        try {
            eg.attachJobGraph(jobVertices);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Building ExecutionGraph failed: " + e.getMessage());
        }

        ExecutionJobVertex execSourceVertex = eg.getJobVertex(sourceVertex.getID());
        ExecutionJobVertex execMapVertex = eg.getJobVertex(mapVertex.getID());
        ExecutionJobVertex execSinkVertex = eg.getJobVertex(sinkVertex.getID());

        assertEquals(0, execSourceVertex.getInputs().size());

        assertEquals(1, execMapVertex.getInputs().size());
        assertEquals(4, execMapVertex.getParallelism());
        ExecutionVertex[] mapTaskVertices = execMapVertex.getTaskVertices();

        // verify that we have each parallel input partition exactly twice, i.e. that one source
        // sends to two unique mappers
        Map<Integer, Integer> mapInputPartitionCounts = new HashMap<>();
        for (ExecutionVertex mapTaskVertex : mapTaskVertices) {
            assertEquals(1, mapTaskVertex.getNumberOfInputs());
            assertEquals(1, mapTaskVertex.getConsumedPartitionGroup(0).size());
            IntermediateResultPartitionID consumedPartitionId =
                    mapTaskVertex.getConsumedPartitionGroup(0).getFirst();
            assertEquals(
                    sourceVertex.getID(),
                    mapTaskVertex
                            .getExecutionGraphAccessor()
                            .getResultPartitionOrThrow(consumedPartitionId)
                            .getProducer()
                            .getJobvertexId());
            int inputPartition = consumedPartitionId.getPartitionNumber();
            if (!mapInputPartitionCounts.containsKey(inputPartition)) {
                mapInputPartitionCounts.put(inputPartition, 1);
            } else {
                mapInputPartitionCounts.put(
                        inputPartition, mapInputPartitionCounts.get(inputPartition) + 1);
            }
        }

        assertEquals(2, mapInputPartitionCounts.size());
        for (int count : mapInputPartitionCounts.values()) {
            assertEquals(2, count);
        }

        assertEquals(1, execSinkVertex.getInputs().size());
        assertEquals(2, execSinkVertex.getParallelism());
        ExecutionVertex[] sinkTaskVertices = execSinkVertex.getTaskVertices();
        InternalExecutionGraphAccessor executionGraphAccessor = execSinkVertex.getGraph();

        // verify each sink instance has two inputs from the map and that each map subpartition
        // only occurs in one unique input edge
        Set<Integer> mapSubpartitions = new HashSet<>();
        for (ExecutionVertex sinkTaskVertex : sinkTaskVertices) {
            assertEquals(1, sinkTaskVertex.getNumberOfInputs());
            assertEquals(2, sinkTaskVertex.getConsumedPartitionGroup(0).size());
            for (IntermediateResultPartitionID consumedPartitionId :
                    sinkTaskVertex.getConsumedPartitionGroup(0)) {
                IntermediateResultPartition consumedPartition =
                        executionGraphAccessor.getResultPartitionOrThrow(consumedPartitionId);
                assertEquals(mapVertex.getID(), consumedPartition.getProducer().getJobvertexId());
                int partitionNumber = consumedPartition.getPartitionNumber();
                assertFalse(mapSubpartitions.contains(partitionNumber));
                mapSubpartitions.add(partitionNumber);
            }
        }

        assertEquals(4, mapSubpartitions.size());
    }
}
