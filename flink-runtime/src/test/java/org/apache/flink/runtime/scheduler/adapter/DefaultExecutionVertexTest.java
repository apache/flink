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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.function.Supplier;

import static org.apache.flink.api.common.InputDependencyConstraint.ANY;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link DefaultExecutionVertex}. */
public class DefaultExecutionVertexTest extends TestLogger {

    private final TestExecutionStateSupplier stateSupplier = new TestExecutionStateSupplier();

    private DefaultExecutionVertex producerVertex;

    private DefaultExecutionVertex consumerVertex;

    private IntermediateResultPartitionID intermediateResultPartitionId;

    @Before
    public void setUp() throws Exception {

        intermediateResultPartitionId = new IntermediateResultPartitionID();

        DefaultResultPartition schedulingResultPartition =
                new DefaultResultPartition(
                        intermediateResultPartitionId,
                        new IntermediateDataSetID(),
                        BLOCKING,
                        () -> ResultPartitionState.CREATED);
        producerVertex =
                new DefaultExecutionVertex(
                        new ExecutionVertexID(new JobVertexID(), 0),
                        Collections.singletonList(schedulingResultPartition),
                        stateSupplier,
                        ANY);
        schedulingResultPartition.setProducer(producerVertex);
        consumerVertex =
                new DefaultExecutionVertex(
                        new ExecutionVertexID(new JobVertexID(), 0),
                        Collections.emptyList(),
                        stateSupplier,
                        ANY);
        consumerVertex.addConsumedResult(schedulingResultPartition);
    }

    @Test
    public void testGetExecutionState() {
        for (ExecutionState state : ExecutionState.values()) {
            stateSupplier.setExecutionState(state);
            assertEquals(state, producerVertex.getState());
        }
    }

    @Test
    public void testGetProducedResultPartitions() {
        IntermediateResultPartitionID partitionIds1 =
                IterableUtils.toStream(producerVertex.getProducedResults())
                        .findAny()
                        .map(SchedulingResultPartition::getId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "can not find result partition"));
        assertEquals(partitionIds1, intermediateResultPartitionId);
    }

    @Test
    public void testGetConsumedResultPartitions() {
        IntermediateResultPartitionID partitionIds1 =
                IterableUtils.toStream(consumerVertex.getConsumedResults())
                        .findAny()
                        .map(SchedulingResultPartition::getId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "can not find result partition"));
        assertEquals(partitionIds1, intermediateResultPartitionId);
    }

    /** A simple implementation of {@link Supplier} for testing. */
    private static class TestExecutionStateSupplier implements Supplier<ExecutionState> {

        private ExecutionState executionState;

        void setExecutionState(ExecutionState state) {
            executionState = state;
        }

        @Override
        public ExecutionState get() {
            return executionState;
        }
    }
}
