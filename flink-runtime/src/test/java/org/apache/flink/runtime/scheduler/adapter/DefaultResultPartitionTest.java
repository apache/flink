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

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link DefaultResultPartition}. */
public class DefaultResultPartitionTest extends TestLogger {

    private static final TestResultPartitionStateSupplier resultPartitionState =
            new TestResultPartitionStateSupplier();

    private final IntermediateResultPartitionID resultPartitionId =
            new IntermediateResultPartitionID();
    private final IntermediateDataSetID intermediateResultId = new IntermediateDataSetID();

    private DefaultResultPartition resultPartition;

    private final Map<IntermediateResultPartitionID, ConsumerVertexGroup> consumerVertexGroups =
            new HashMap<>();

    @Before
    public void setUp() {
        resultPartition =
                new DefaultResultPartition(
                        resultPartitionId,
                        intermediateResultId,
                        BLOCKING,
                        resultPartitionState,
                        () -> consumerVertexGroups.get(resultPartitionId),
                        () -> {
                            throw new UnsupportedOperationException();
                        });
    }

    @Test
    public void testGetPartitionState() {
        for (ResultPartitionState state : ResultPartitionState.values()) {
            resultPartitionState.setResultPartitionState(state);
            assertEquals(state, resultPartition.getState());
        }
    }

    @Test
    public void testGetConsumerVertexGroup() {

        assertFalse(resultPartition.getConsumerVertexGroup().isPresent());

        // test update consumers
        ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        consumerVertexGroups.put(
                resultPartition.getId(), ConsumerVertexGroup.fromSingleVertex(executionVertexId));
        assertTrue(resultPartition.getConsumerVertexGroup().isPresent());
        assertThat(resultPartition.getConsumerVertexGroup().get(), contains(executionVertexId));
    }

    /** A test {@link ResultPartitionState} supplier. */
    private static class TestResultPartitionStateSupplier
            implements Supplier<ResultPartitionState> {

        private ResultPartitionState resultPartitionState;

        void setResultPartitionState(ResultPartitionState state) {
            resultPartitionState = state;
        }

        @Override
        public ResultPartitionState get() {
            return resultPartitionState;
        }
    }
}
