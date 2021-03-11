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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.IterableUtils.flatMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link SchedulingExecutionVertex}. */
class DefaultExecutionVertex implements SchedulingExecutionVertex {

    private final ExecutionVertexID executionVertexId;

    private final List<DefaultResultPartition> producedResults;

    private final Supplier<ExecutionState> stateSupplier;

    private final List<ConsumedPartitionGroup> consumedPartitionGroups;

    private final Function<IntermediateResultPartitionID, DefaultResultPartition>
            resultPartitionRetriever;

    DefaultExecutionVertex(
            ExecutionVertexID executionVertexId,
            List<DefaultResultPartition> producedPartitions,
            Supplier<ExecutionState> stateSupplier,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {
        this.executionVertexId = checkNotNull(executionVertexId);
        this.stateSupplier = checkNotNull(stateSupplier);
        this.producedResults = checkNotNull(producedPartitions);
        this.consumedPartitionGroups = consumedPartitionGroups;
        this.resultPartitionRetriever = resultPartitionRetriever;
    }

    @VisibleForTesting
    DefaultExecutionVertex(
            ExecutionVertexID executionVertexId,
            List<DefaultResultPartition> producedPartitions,
            Supplier<ExecutionState> stateSupplier) {
        this(executionVertexId, producedPartitions, stateSupplier, null, null);
    }

    @Override
    public ExecutionVertexID getId() {
        return executionVertexId;
    }

    @Override
    public ExecutionState getState() {
        return stateSupplier.get();
    }

    @Override
    public Iterable<DefaultResultPartition> getConsumedResults() {
        return () -> flatMap(consumedPartitionGroups, resultPartitionRetriever);
    }

    @Override
    public List<ConsumedPartitionGroup> getConsumedPartitionGroups() {
        return consumedPartitionGroups;
    }

    @Override
    public Iterable<DefaultResultPartition> getProducedResults() {
        return producedResults;
    }
}
