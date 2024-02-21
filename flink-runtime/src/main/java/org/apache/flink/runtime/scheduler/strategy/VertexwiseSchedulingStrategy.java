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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of vertex (which
 * indicates this strategy only supports batch jobs). Note that this strategy implements {@link
 * SchedulingTopologyListener}, so it can handle the updates of scheduling topology.
 */
public class VertexwiseSchedulingStrategy
        implements SchedulingStrategy, SchedulingTopologyListener {

    private static final Logger LOG = LoggerFactory.getLogger(VertexwiseSchedulingStrategy.class);

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    private final Set<ExecutionVertexID> newVertices = new HashSet<>();

    private final Set<ExecutionVertexID> scheduledVertices = new HashSet<>();

    private final InputConsumableDecider inputConsumableDecider;

    public VertexwiseSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology,
            final InputConsumableDecider.Factory inputConsumableDeciderFactory) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.inputConsumableDecider =
                inputConsumableDeciderFactory.createInstance(
                        schedulingTopology, scheduledVertices::contains);
        LOG.info(
                "Using InputConsumableDecider {} for VertexwiseSchedulingStrategy.",
                inputConsumableDecider.getClass().getName());
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
        scheduledVertices.removeAll(verticesToRestart);
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
                            .filter(
                                    group ->
                                            inputConsumableDecider
                                                    .isConsumableBasedOnFinishedProducers(
                                                            group.getConsumedPartitionGroup()))
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
        Set<ExecutionVertexID> allCandidates;
        if (newVertices.isEmpty()) {
            allCandidates = vertices;
        } else {
            allCandidates = new HashSet<>(vertices);
            allCandidates.addAll(newVertices);
            newVertices.clear();
        }

        final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();

        Set<ExecutionVertexID> nextVertices = allCandidates;
        while (!nextVertices.isEmpty()) {
            nextVertices = addToScheduleAndGetVertices(nextVertices, verticesToSchedule);
        }

        scheduleVerticesOneByOne(verticesToSchedule);
        scheduledVertices.addAll(verticesToSchedule);
    }

    private Set<ExecutionVertexID> addToScheduleAndGetVertices(
            Set<ExecutionVertexID> currentVertices, Set<ExecutionVertexID> verticesToSchedule) {
        Set<ExecutionVertexID> nextVertices = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new IdentityHashMap<>();
        final Set<ConsumerVertexGroup> visitedConsumerVertexGroup =
                Collections.newSetFromMap(new IdentityHashMap<>());

        for (ExecutionVertexID currentVertex : currentVertices) {
            if (isVertexSchedulable(currentVertex, consumableStatusCache, verticesToSchedule)) {
                verticesToSchedule.add(currentVertex);
                Set<ConsumerVertexGroup> canBePipelinedConsumerVertexGroups =
                        IterableUtils.toStream(
                                        schedulingTopology
                                                .getVertex(currentVertex)
                                                .getProducedResults())
                                .map(SchedulingResultPartition::getConsumerVertexGroups)
                                .flatMap(Collection::stream)
                                .filter(
                                        (consumerVertexGroup) ->
                                                consumerVertexGroup
                                                        .getResultPartitionType()
                                                        .canBePipelinedConsumed())
                                .collect(Collectors.toSet());
                for (ConsumerVertexGroup consumerVertexGroup : canBePipelinedConsumerVertexGroups) {
                    if (!visitedConsumerVertexGroup.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroup.add(consumerVertexGroup);
                        nextVertices.addAll(
                                IterableUtils.toStream(consumerVertexGroup)
                                        .collect(Collectors.toSet()));
                    }
                }
            }
        }
        return nextVertices;
    }

    private boolean isVertexSchedulable(
            final ExecutionVertexID vertex,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<ExecutionVertexID> verticesToSchedule) {
        return !verticesToSchedule.contains(vertex)
                && !scheduledVertices.contains(vertex)
                && inputConsumableDecider.isInputConsumable(
                        schedulingTopology.getVertex(vertex),
                        verticesToSchedule,
                        consumableStatusCache);
    }

    private void scheduleVerticesOneByOne(final Set<ExecutionVertexID> verticesToSchedule) {
        if (verticesToSchedule.isEmpty()) {
            return;
        }
        final List<ExecutionVertexID> sortedVerticesToSchedule =
                SchedulingStrategyUtils.sortExecutionVerticesInTopologicalOrder(
                        schedulingTopology, verticesToSchedule);

        sortedVerticesToSchedule.forEach(
                id -> schedulerOperations.allocateSlotsAndDeploy(Collections.singletonList(id)));
    }

    /** The factory for creating {@link VertexwiseSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        private final InputConsumableDecider.Factory inputConsumableDeciderFactory;

        public Factory(InputConsumableDecider.Factory inputConsumableDeciderFactory) {
            this.inputConsumableDeciderFactory = inputConsumableDeciderFactory;
        }

        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new VertexwiseSchedulingStrategy(
                    schedulerOperations, schedulingTopology, inputConsumableDeciderFactory);
        }
    }
}
