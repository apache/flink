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
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link DefaultExecutionVertex}. */
class DefaultExecutionVertexTest {

    private final TestExecutionStateSupplier stateSupplier = new TestExecutionStateSupplier();

    private DefaultExecutionVertex producerVertex;

    private DefaultExecutionVertex consumerVertex;

    private IntermediateResultPartitionID intermediateResultPartitionId;

    @BeforeEach
    void setUp() throws Exception {

        intermediateResultPartitionId = new IntermediateResultPartitionID();

        DefaultResultPartition schedulingResultPartition =
                new DefaultResultPartition(
                        intermediateResultPartitionId,
                        new IntermediateDataSetID(),
                        BLOCKING,
                        () -> ResultPartitionState.CREATED,
                        () -> {
                            throw new UnsupportedOperationException();
                        },
                        () -> {
                            throw new UnsupportedOperationException();
                        });
        producerVertex =
                new DefaultExecutionVertex(
                        new ExecutionVertexID(new JobVertexID(), 0),
                        Collections.singletonList(schedulingResultPartition),
                        stateSupplier,
                        Collections.emptyList(),
                        partitionID -> {
                            throw new UnsupportedOperationException();
                        });
        schedulingResultPartition.setProducer(producerVertex);

        List<ConsumedPartitionGroup> consumedPartitionGroups =
                Collections.singletonList(
                        ConsumedPartitionGroup.fromSinglePartition(
                                1,
                                intermediateResultPartitionId,
                                schedulingResultPartition.getResultType()));
        Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionById =
                Collections.singletonMap(intermediateResultPartitionId, schedulingResultPartition);

        consumerVertex =
                new DefaultExecutionVertex(
                        new ExecutionVertexID(new JobVertexID(), 0),
                        Collections.emptyList(),
                        stateSupplier,
                        consumedPartitionGroups,
                        resultPartitionById::get);
    }

    @Test
    void testGetExecutionState() {
        for (ExecutionState state : ExecutionState.values()) {
            stateSupplier.setExecutionState(state);
            assertThat(producerVertex.getState()).isEqualTo(state);
        }
    }

    @Test
    void testGetProducedResultPartitions() {
        IntermediateResultPartitionID partitionIds1 =
                IterableUtils.toStream(producerVertex.getProducedResults())
                        .findAny()
                        .map(SchedulingResultPartition::getId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "can not find result partition"));
        assertThat(intermediateResultPartitionId).isEqualTo(partitionIds1);
    }

    @Test
    void testGetConsumedResultPartitions() {
        IntermediateResultPartitionID partitionIds1 =
                IterableUtils.toStream(consumerVertex.getConsumedResults())
                        .findAny()
                        .map(SchedulingResultPartition::getId)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "can not find result partition"));
        assertThat(intermediateResultPartitionId).isEqualTo(partitionIds1);
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
