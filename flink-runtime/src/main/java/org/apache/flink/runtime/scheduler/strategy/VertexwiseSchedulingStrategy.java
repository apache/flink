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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.util.IterableUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of vertex (which
 * indicates this strategy only supports ALL_EDGES_BLOCKING batch jobs). Note that this strategy
 * implements {@link SchedulingTopologyListener}, so it can handle the updates of scheduling
 * topology.
 */
public class VertexwiseSchedulingStrategy
        implements SchedulingStrategy, SchedulingTopologyListener {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final Set<ExecutionVertexID> newVertices = new HashSet<>();

    public VertexwiseSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
        schedulingTopology.registerSchedulingTopologyListener(this);
    }

    @Override
    public void startScheduling() {
        Set<ExecutionVertexID> sourceVertices =
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .filter(vertex -> vertex.getConsumedPartitionGroups().isEmpty())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        maybeScheduleVertices(sourceVertices);
    }

    @Override
    public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
        maybeScheduleVertices(verticesToRestart);
    }

    @Override
    public void onExecutionStateChange(
            ExecutionVertexID executionVertexId, ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            SchedulingExecutionVertex executionVertex =
                    schedulingTopology.getVertex(executionVertexId);

            Set<ExecutionVertexID> consumerVertices =
                    IterableUtils.toStream(executionVertex.getProducedResults())
                            .map(SchedulingResultPartition::getConsumerVertexGroups)
                            .flatMap(Collection::stream)
                            .flatMap(IterableUtils::toStream)
                            .collect(Collectors.toSet());

            maybeScheduleVertices(consumerVertices);
        }
    }

    @Override
    public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {}

    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {
        checkState(schedulingTopology == this.schedulingTopology);
        newVertices.addAll(newExecutionVertices);
    }

    private void maybeScheduleVertices(final Set<ExecutionVertexID> vertices) {
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();

        Set<ExecutionVertexID> allCandidates;
        if (newVertices.isEmpty()) {
            allCandidates = vertices;
        } else {
            allCandidates = new HashSet<>(vertices);
            allCandidates.addAll(newVertices);
            newVertices.clear();
        }

        final Set<ExecutionVertexID> verticesToDeploy =
                allCandidates.stream()
                        .filter(
                                vertexId -> {
                                    SchedulingExecutionVertex vertex =
                                            schedulingTopology.getVertex(vertexId);
                                    checkState(vertex.getState() == ExecutionState.CREATED);
                                    return areVertexInputsAllConsumable(
                                            vertex, consumableStatusCache);
                                })
                        .collect(Collectors.toSet());

        scheduleVerticesOneByOne(verticesToDeploy);
    }

    private void scheduleVerticesOneByOne(final Set<ExecutionVertexID> verticesToDeploy) {
        if (verticesToDeploy.isEmpty()) {
            return;
        }
        final List<ExecutionVertexID> sortedVerticesToDeploy =
                SchedulingStrategyUtils.sortExecutionVerticesInTopologicalOrder(
                        schedulingTopology, verticesToDeploy);

        sortedVerticesToDeploy.forEach(
                id -> schedulerOperations.allocateSlotsAndDeploy(Collections.singletonList(id)));
    }

    private boolean areVertexInputsAllConsumable(
            SchedulingExecutionVertex vertex,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {
        for (ConsumedPartitionGroup consumedPartitionGroup : vertex.getConsumedPartitionGroups()) {

            if (!consumableStatusCache.computeIfAbsent(
                    consumedPartitionGroup, this::isConsumedPartitionGroupConsumable)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConsumedPartitionGroupConsumable(
            final ConsumedPartitionGroup consumedPartitionGroup) {
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            if (schedulingTopology.getResultPartition(partitionId).getState()
                    != ResultPartitionState.CONSUMABLE) {
                return false;
            }
        }
        return true;
    }

    /** The factory for creating {@link VertexwiseSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new VertexwiseSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
