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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This strategy tries to get a balanced tasks scheduling. Execution vertices, which are belong to
 * the same SlotSharingGroup, tend to be put evenly in each ExecutionSlotSharingGroup. Co-location
 * constraints will be respected.
 */
class TaskBalancedPreferredSlotSharingStrategy extends AbstractSlotSharingStrategy {

    public static final Logger LOG =
            LoggerFactory.getLogger(TaskBalancedPreferredSlotSharingStrategy.class);

    TaskBalancedPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {
        super(topology, slotSharingGroups, coLocationGroups);
    }

    @Override
    protected Map<ExecutionVertexID, ExecutionSlotSharingGroup> computeExecutionSlotSharingGroups(
            SchedulingTopology schedulingTopology) {
        return new TaskBalancedExecutionSlotSharingGroupBuilder(
                        schedulingTopology, this.logicalSlotSharingGroups, this.coLocationGroups)
                .build();
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public TaskBalancedPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> slotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new TaskBalancedPreferredSlotSharingStrategy(
                    topology, slotSharingGroups, coLocationGroups);
        }
    }

    /** The interface to compute the fittest slot index. */
    private interface SlotIndexSupplier {

        int getFittestSlotIndex(
                final SlotSharingGroup slotSharingGroup,
                @Nullable final SchedulingExecutionVertex executionVertex);
    }

    /** SlotSharingGroupBuilder class for balanced scheduling strategy. */
    private static class TaskBalancedExecutionSlotSharingGroupBuilder {

        private final SchedulingTopology topology;

        private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

        /** Record the {@link ExecutionSlotSharingGroup}s for {@link SlotSharingGroup}s. */
        private final Map<SlotSharingGroup, List<ExecutionSlotSharingGroup>>
                paralleledExecutionSlotSharingGroupsMap;

        /**
         * Record the next round-robin {@link ExecutionSlotSharingGroup} index for {@link
         * SlotSharingGroup}s.
         */
        private final Map<SlotSharingGroup, Integer> slotSharingGroupIndexMap;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        private final Map<JobVertexID, CoLocationGroup> coLocationGroupMap;

        private final Map<CoLocationConstraint, ExecutionSlotSharingGroup>
                constraintToExecutionSlotSharingGroupMap;

        private TaskBalancedExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> slotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {
            this.topology = checkNotNull(topology);

            this.coLocationGroupMap = new HashMap<>();
            for (CoLocationGroup coLocationGroup : coLocationGroups) {
                for (JobVertexID jobVertexId : coLocationGroup.getVertexIds()) {
                    coLocationGroupMap.put(jobVertexId, coLocationGroup);
                }
            }

            this.constraintToExecutionSlotSharingGroupMap = new HashMap<>();
            this.paralleledExecutionSlotSharingGroupsMap = new HashMap<>(slotSharingGroups.size());
            this.slotSharingGroupIndexMap = new HashMap<>(slotSharingGroups.size());
            this.slotSharingGroupMap = new HashMap<>();
            this.executionSlotSharingGroupMap = new HashMap<>();

            for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup);
                }
            }
        }

        private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {

            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices =
                    getExecutionVertices(topology);

            initParalleledExecutionSlotSharingGroupsMap(allVertices);

            // Loop on job vertices
            for (Map.Entry<JobVertexID, List<SchedulingExecutionVertex>> executionVertexInfos :
                    allVertices.entrySet()) {

                JobVertexID jobVertexID = executionVertexInfos.getKey();
                List<SchedulingExecutionVertex> executionVertices = executionVertexInfos.getValue();
                final SlotSharingGroup slotSharingGroup = slotSharingGroupMap.get(jobVertexID);

                if (!coLocationGroupMap.containsKey(jobVertexID)) {
                    // For vertices without CoLocationConstraint.
                    allocateNonCoLocatedVertices(slotSharingGroup, executionVertices);
                } else {
                    // For vertices with CoLocationConstraint.
                    allocateCoLocatedVertices(slotSharingGroup, executionVertices);
                }
            }
            return executionSlotSharingGroupMap;
        }

        private void initParalleledExecutionSlotSharingGroupsMap(
                final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices) {

            allVertices.entrySet().stream()
                    .map(
                            jobVertexExecutionVertices ->
                                    Tuple2.of(
                                            slotSharingGroupMap.get(
                                                    jobVertexExecutionVertices.getKey()),
                                            jobVertexExecutionVertices.getValue().size()))
                    .collect(
                            Collectors.groupingBy(
                                    tuple -> tuple.f0,
                                    Collectors.summarizingInt(tuple -> tuple.f1)))
                    .forEach(
                            (slotSharingGroup, statistics) -> {
                                int slotNum = statistics.getMax();
                                paralleledExecutionSlotSharingGroupsMap.put(
                                        slotSharingGroup,
                                        createExecutionSlotSharingGroups(
                                                slotSharingGroup, slotNum));
                            });
        }

        private List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
                SlotSharingGroup slotSharingGroup, int slotNum) {
            final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                    new ArrayList<>(slotNum);
            for (int i = 0; i < slotNum; i++) {
                final ExecutionSlotSharingGroup executionSlotSharingGroup =
                        new ExecutionSlotSharingGroup(slotSharingGroup);
                executionSlotSharingGroups.add(i, executionSlotSharingGroup);
                LOG.debug(
                        "Create {}th executionSlotSharingGroup {}.", i, executionSlotSharingGroup);
            }
            return executionSlotSharingGroups;
        }

        private void allocateCoLocatedVertices(
                SlotSharingGroup slotSharingGroup,
                List<SchedulingExecutionVertex> executionVertices) {
            if (isMaxParallelism(executionVertices.size(), slotSharingGroup)) {
                // For CoLocations with max parallelism of the slot sharing group.
                allocateCoLocatedVertices(
                        slotSharingGroup,
                        executionVertices,
                        (sharingGroup, executionVertex) ->
                                checkNotNull(executionVertex).getId().getSubtaskIndex());
            } else {
                // For CoLocations with non-max parallelism of the slot sharing group.
                allocateCoLocatedVertices(
                        slotSharingGroup,
                        executionVertices,
                        (sharingGroup, executionVertex) ->
                                getLeastUtilizeSlotIndex(
                                        paralleledExecutionSlotSharingGroupsMap.get(sharingGroup),
                                        executionVertex));
            }
        }

        private void allocateCoLocatedVertices(
                SlotSharingGroup slotSharingGroup,
                List<SchedulingExecutionVertex> executionVertices,
                @Nonnull SlotIndexSupplier slotIndexSupplier) {
            final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                    paralleledExecutionSlotSharingGroupsMap.get(slotSharingGroup);
            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                final CoLocationConstraint coLocationConstraint =
                        getCoLocationConstraint(executionVertex);
                ExecutionSlotSharingGroup executionSlotSharingGroup =
                        constraintToExecutionSlotSharingGroupMap.get(coLocationConstraint);
                if (Objects.isNull(executionSlotSharingGroup)) {
                    executionSlotSharingGroup =
                            executionSlotSharingGroups.get(
                                    slotIndexSupplier.getFittestSlotIndex(
                                            slotSharingGroup, executionVertex));
                    constraintToExecutionSlotSharingGroupMap.put(
                            coLocationConstraint, executionSlotSharingGroup);
                }
                addVertexToExecutionSlotSharingGroup(executionSlotSharingGroup, executionVertex);
            }
            final int jobVertexParallel = executionVertices.size();
            if (isMaxParallelism(jobVertexParallel, slotSharingGroup)) {
                return;
            }
            int index = slotIndexSupplier.getFittestSlotIndex(slotSharingGroup, null);
            updateSlotRoundRobinIndexIfNeeded(jobVertexParallel, slotSharingGroup, index);
        }

        private void allocateNonCoLocatedVertices(
                SlotSharingGroup slotSharingGroup,
                List<SchedulingExecutionVertex> executionVertices) {
            final int jobVertexParallel = executionVertices.size();
            int index = getSlotRoundRobinIndex(jobVertexParallel, slotSharingGroup);
            final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                    paralleledExecutionSlotSharingGroupsMap.get(slotSharingGroup);
            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                addVertexToExecutionSlotSharingGroup(
                        executionSlotSharingGroups.get(index), executionVertex);
                index = ++index % executionSlotSharingGroups.size();
            }
            updateSlotRoundRobinIndexIfNeeded(executionVertices.size(), slotSharingGroup, index);
        }

        private void addVertexToExecutionSlotSharingGroup(
                ExecutionSlotSharingGroup executionSlotSharingGroup,
                SchedulingExecutionVertex executionVertex) {
            final ExecutionVertexID executionVertexId = executionVertex.getId();
            executionSlotSharingGroup.addVertex(executionVertexId);
            executionSlotSharingGroupMap.put(executionVertexId, executionSlotSharingGroup);
        }

        private CoLocationConstraint getCoLocationConstraint(SchedulingExecutionVertex sev) {
            final JobVertexID jobVertexID = sev.getId().getJobVertexId();
            final int subtaskIndex = sev.getId().getSubtaskIndex();
            return coLocationGroupMap.get(jobVertexID).getLocationConstraint(subtaskIndex);
        }

        private int getSlotRoundRobinIndex(
                final int jobVertexParallelism, SlotSharingGroup slotSharingGroup) {
            final boolean maxParallel = isMaxParallelism(jobVertexParallelism, slotSharingGroup);
            return maxParallel ? 0 : slotSharingGroupIndexMap.getOrDefault(slotSharingGroup, 0);
        }

        private void updateSlotRoundRobinIndexIfNeeded(
                final int jobVertexParallelism,
                final SlotSharingGroup slotSharingGroup,
                final int nextIndex) {
            if (!isMaxParallelism(jobVertexParallelism, slotSharingGroup)) {
                slotSharingGroupIndexMap.put(slotSharingGroup, nextIndex);
            }
        }

        private boolean isMaxParallelism(
                final int jobVertexParallelism, final SlotSharingGroup slotSharingGroup) {
            final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                    paralleledExecutionSlotSharingGroupsMap.get(slotSharingGroup);
            return jobVertexParallelism == executionSlotSharingGroups.size();
        }

        private int getLeastUtilizeSlotIndex(
                final List<ExecutionSlotSharingGroup> executionSlotSharingGroups,
                @Nullable final SchedulingExecutionVertex executionVertex) {
            int indexWithLeastExecutionVertices = 0;
            int leastExecutionVertices = Integer.MAX_VALUE;
            for (int index = 0; index < executionSlotSharingGroups.size(); index++) {
                final ExecutionSlotSharingGroup executionSlotSharingGroup =
                        executionSlotSharingGroups.get(index);
                final int executionVertices =
                        executionSlotSharingGroup.getExecutionVertexIds().size();
                if (leastExecutionVertices > executionVertices
                        && (Objects.isNull(executionVertex)
                                || allocatable(executionSlotSharingGroup, executionVertex))) {
                    indexWithLeastExecutionVertices = index;
                    leastExecutionVertices = executionVertices;
                }
            }
            return indexWithLeastExecutionVertices;
        }

        private boolean allocatable(
                final ExecutionSlotSharingGroup executionSlotSharingGroup,
                @Nonnull SchedulingExecutionVertex executionVertex) {
            final ExecutionVertexID executionVertexId = executionVertex.getId();
            final JobVertexID jobVertexId = executionVertexId.getJobVertexId();
            final Set<ExecutionVertexID> existedExecutionVertexIds =
                    executionSlotSharingGroup.getExecutionVertexIds();
            final Set<JobVertexID> allocatedJobVertices =
                    executionSlotSharingGroup.getExecutionVertexIds().stream()
                            .map(ExecutionVertexID::getJobVertexId)
                            .collect(Collectors.toSet());
            return !existedExecutionVertexIds.contains(executionVertexId)
                    && !allocatedJobVertices.contains(jobVertexId);
        }
    }
}
