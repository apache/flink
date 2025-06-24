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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology.connectConsumersToProducersById;

/** A simple inputs locations retriever for testing purposes. */
class TestingInputsLocationsRetriever implements InputsLocationsRetriever {

    private final Map<ExecutionVertexID, Collection<ConsumedPartitionGroup>>
            vertexToConsumedPartitionGroups;

    private final Map<IntermediateResultPartitionID, ExecutionVertexID> partitionToProducer;

    private final Map<ExecutionVertexID, CompletableFuture<TaskManagerLocation>>
            taskManagerLocationsByVertex = new HashMap<>();

    TestingInputsLocationsRetriever(
            final Map<ExecutionVertexID, Collection<ConsumedPartitionGroup>>
                    vertexToConsumedPartitionGroups,
            final Map<IntermediateResultPartitionID, ExecutionVertexID> partitionToProducer) {

        this.vertexToConsumedPartitionGroups = vertexToConsumedPartitionGroups;
        this.partitionToProducer = partitionToProducer;
    }

    @Override
    public Collection<ConsumedPartitionGroup> getConsumedPartitionGroups(
            final ExecutionVertexID executionVertexId) {
        return vertexToConsumedPartitionGroups.get(executionVertexId);
    }

    @Override
    public Collection<ExecutionVertexID> getProducersOfConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        return IterableUtils.toStream(consumedPartitionGroup)
                .map(partitionToProducer::get)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CompletableFuture<TaskManagerLocation>> getTaskManagerLocation(
            final ExecutionVertexID executionVertexId) {
        return Optional.ofNullable(taskManagerLocationsByVertex.get(executionVertexId));
    }

    public void markScheduled(final ExecutionVertexID executionVertexId) {
        taskManagerLocationsByVertex.put(executionVertexId, new CompletableFuture<>());
    }

    public void assignTaskManagerLocation(final ExecutionVertexID executionVertexId) {
        assignTaskManagerLocation(executionVertexId, new LocalTaskManagerLocation());
    }

    public void assignTaskManagerLocation(
            final ExecutionVertexID executionVertexId, TaskManagerLocation location) {
        taskManagerLocationsByVertex.compute(
                executionVertexId,
                (key, future) -> {
                    if (future == null) {
                        return CompletableFuture.completedFuture(location);
                    }
                    future.complete(location);
                    return future;
                });
    }

    void failTaskManagerLocation(final ExecutionVertexID executionVertexId, final Throwable cause) {
        taskManagerLocationsByVertex.compute(
                executionVertexId,
                (key, future) -> {
                    CompletableFuture<TaskManagerLocation> futureToFail = future;
                    if (futureToFail == null) {
                        futureToFail = new CompletableFuture<>();
                    }
                    futureToFail.completeExceptionally(cause);
                    return futureToFail;
                });
    }

    void cancelTaskManagerLocation(final ExecutionVertexID executionVertexId) {
        taskManagerLocationsByVertex.compute(
                executionVertexId,
                (key, future) -> {
                    CompletableFuture<TaskManagerLocation> futureToCancel = future;
                    if (futureToCancel == null) {
                        futureToCancel = new CompletableFuture<>();
                    }
                    futureToCancel.cancel(true);
                    return futureToCancel;
                });
    }

    static class Builder {

        private final Map<ExecutionVertexID, Collection<ConsumedPartitionGroup>>
                vertexToConsumedPartitionGroups = new HashMap<>();

        private final Map<IntermediateResultPartitionID, ExecutionVertexID> partitionToProducer =
                new HashMap<>();

        public Builder connectConsumerToProducer(
                final ExecutionVertexID consumer, final ExecutionVertexID producer) {
            return connectConsumerToProducers(consumer, Collections.singletonList(producer));
        }

        public Builder connectConsumerToProducers(
                final ExecutionVertexID consumer, final List<ExecutionVertexID> producers) {
            return connectConsumersToProducers(Collections.singletonList(consumer), producers);
        }

        public Builder connectConsumersToProducers(
                final List<ExecutionVertexID> consumers, final List<ExecutionVertexID> producers) {
            TestingSchedulingTopology.ConnectionResult connectionResult =
                    connectConsumersToProducersById(
                            consumers,
                            producers,
                            new IntermediateDataSetID(),
                            ResultPartitionType.PIPELINED);

            for (int i = 0; i < producers.size(); i++) {
                partitionToProducer.put(
                        connectionResult.getResultPartitions().get(i), producers.get(i));
            }

            for (ExecutionVertexID consumer : consumers) {
                final Collection<ConsumedPartitionGroup> consumedPartitionGroups =
                        vertexToConsumedPartitionGroups.computeIfAbsent(
                                consumer, ignore -> new ArrayList<>());
                consumedPartitionGroups.add(connectionResult.getConsumedPartitionGroup());
            }

            return this;
        }

        public TestingInputsLocationsRetriever build() {
            return new TestingInputsLocationsRetriever(
                    vertexToConsumedPartitionGroups, partitionToProducer);
        }
    }
}
