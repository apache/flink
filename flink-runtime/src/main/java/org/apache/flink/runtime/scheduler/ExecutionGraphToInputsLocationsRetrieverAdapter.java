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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.IterableUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An implementation of {@link InputsLocationsRetriever} based on the {@link ExecutionGraph}. */
public class ExecutionGraphToInputsLocationsRetrieverAdapter implements InputsLocationsRetriever {

    private final ExecutionGraph executionGraph;

    public ExecutionGraphToInputsLocationsRetrieverAdapter(ExecutionGraph executionGraph) {
        this.executionGraph = checkNotNull(executionGraph);
    }

    @Override
    public Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
            ExecutionVertexID executionVertexId) {
        return getExecutionVertex(executionVertexId).getAllConsumedPartitionGroups();
    }

    @Override
    public Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        return IterableUtils.toStream(consumedPartitionGroup)
                .map(
                        partition ->
                                executionGraph
                                        .getResultPartitionOrThrow(partition)
                                        .getProducer()
                                        .getID())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
            ExecutionVertexID executionVertexId) {
        ExecutionVertex ev = getExecutionVertex(executionVertexId);

        if (ev.getExecutionState() != ExecutionState.CREATED) {
            return Optional.of(ev.getCurrentTaskManagerLocationFuture());
        } else {
            return Optional.empty();
        }
    }

    private ExecutionVertex getExecutionVertex(ExecutionVertexID executionVertexId) {
        ExecutionJobVertex ejv = executionGraph.getJobVertex(executionVertexId.getJobVertexId());

        checkState(
                ejv != null && ejv.getParallelism() > executionVertexId.getSubtaskIndex(),
                "Failed to find execution %s in execution graph.",
                executionVertexId);

        return ejv.getTaskVertices()[executionVertexId.getSubtaskIndex()];
    }
}
