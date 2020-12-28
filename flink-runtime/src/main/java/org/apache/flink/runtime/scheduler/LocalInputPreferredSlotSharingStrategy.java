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

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraintDesc;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupDesc;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This strategy tries to reduce remote data exchanges. Execution vertices, which are connected and
 * belong to the same SlotSharingGroup, tend to be put in the same ExecutionSlotSharingGroup.
 * Co-location constraints will be respected.
 */
class LocalInputPreferredSlotSharingStrategy implements SlotSharingStrategy {

    private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    LocalInputPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> logicalSlotSharingGroups,
            final Set<CoLocationGroupDesc> coLocationGroups) {

        this.executionSlotSharingGroupMap =
                new ExecutionSlotSharingGroupBuilder(
                                topology, logicalSlotSharingGroups, coLocationGroups)
                        .build();
    }

    @Override
    public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
            final ExecutionVertexID executionVertexId) {
        return executionSlotSharingGroupMap.get(executionVertexId);
    }

    @Override
    public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
        return new HashSet<>(executionSlotSharingGroupMap.values());
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public LocalInputPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroupDesc> coLocationGroups) {

            return new LocalInputPreferredSlotSharingStrategy(
                    topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    private static class ExecutionSlotSharingGroupBuilder {
        private final SchedulingTopology topology;

        private final Map<JobVertexID, SlotSharingGroupId> slotSharingGroupMap;

        private final Map<JobVertexID, CoLocationGroupDesc> coLocationGroupMap;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        final Map<CoLocationConstraintDesc, ExecutionSlotSharingGroup>
                constraintToExecutionSlotSharingGroupMap;

        final Map<SlotSharingGroupId, List<ExecutionSlotSharingGroup>> executionSlotSharingGroups;

        private final Map<ExecutionSlotSharingGroup, Set<JobVertexID>> assignedJobVerticesForGroups;

        private ExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroupDesc> coLocationGroups) {

            this.topology = checkNotNull(topology);

            this.slotSharingGroupMap = new HashMap<>();
            for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup.getSlotSharingGroupId());
                }
            }

            this.coLocationGroupMap = new HashMap<>();
            for (CoLocationGroupDesc coLocationGroup : coLocationGroups) {
                for (JobVertexID jobVertexId : coLocationGroup.getVertices()) {
                    coLocationGroupMap.put(jobVertexId, coLocationGroup);
                }
            }

            executionSlotSharingGroupMap = new HashMap<>();
            constraintToExecutionSlotSharingGroupMap = new HashMap<>();
            executionSlotSharingGroups = new HashMap<>();
            assignedJobVerticesForGroups = new IdentityHashMap<>();
        }

        /**
         * Build ExecutionSlotSharingGroups for all vertices in the topology. The
         * ExecutionSlotSharingGroup of a vertex is determined in order below:
         *
         * <p>1. try finding an existing group of the corresponding co-location constraint.
         *
         * <p>2. try finding an available group of its producer vertex if the producer is in the
         * same slot sharing group.
         *
         * <p>3. try finding any available group.
         *
         * <p>4. create a new group.
         */
        private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices =
                    getExecutionVertices();

            // loop on job vertices so that an execution vertex will not be added into a group
            // if that group better fits another execution vertex
            for (List<SchedulingExecutionVertex> executionVertices : allVertices.values()) {
                final List<SchedulingExecutionVertex> remaining =
                        tryFindOptimalAvailableExecutionSlotSharingGroupFor(executionVertices);

                findAvailableOrCreateNewExecutionSlotSharingGroupFor(remaining);

                updateConstraintToExecutionSlotSharingGroupMap(executionVertices);
            }

            return executionSlotSharingGroupMap;
        }

        private LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> getExecutionVertices() {
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> vertices =
                    new LinkedHashMap<>();
            for (SchedulingExecutionVertex executionVertex : topology.getVertices()) {
                final List<SchedulingExecutionVertex> executionVertexGroup =
                        vertices.computeIfAbsent(
                                executionVertex.getId().getJobVertexId(), k -> new ArrayList<>());
                executionVertexGroup.add(executionVertex);
            }
            return vertices;
        }

        private List<SchedulingExecutionVertex> tryFindOptimalAvailableExecutionSlotSharingGroupFor(
                final List<SchedulingExecutionVertex> executionVertices) {

            final List<SchedulingExecutionVertex> remaining = new ArrayList<>();
            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                ExecutionSlotSharingGroup group =
                        tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(executionVertex);

                if (group == null) {
                    group = tryFindAvailableProducerExecutionSlotSharingGroupFor(executionVertex);
                }

                if (group == null) {
                    remaining.add(executionVertex);
                } else {
                    addVertexToExecutionSlotSharingGroup(executionVertex, group);
                }
            }

            return remaining;
        }

        private ExecutionSlotSharingGroup tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(
                final SchedulingExecutionVertex executionVertex) {

            final ExecutionVertexID executionVertexId = executionVertex.getId();
            final CoLocationGroupDesc coLocationGroup =
                    coLocationGroupMap.get(executionVertexId.getJobVertexId());
            if (coLocationGroup != null) {
                final CoLocationConstraintDesc constraint =
                        coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());

                return constraintToExecutionSlotSharingGroupMap.get(constraint);
            } else {
                return null;
            }
        }

        private ExecutionSlotSharingGroup tryFindAvailableProducerExecutionSlotSharingGroupFor(
                final SchedulingExecutionVertex executionVertex) {

            final ExecutionVertexID executionVertexId = executionVertex.getId();

            for (SchedulingResultPartition partition : executionVertex.getConsumedResults()) {
                final ExecutionVertexID producerVertexId = partition.getProducer().getId();
                if (!inSameLogicalSlotSharingGroup(producerVertexId, executionVertexId)) {
                    continue;
                }

                final ExecutionSlotSharingGroup producerGroup =
                        executionSlotSharingGroupMap.get(producerVertexId);

                checkState(producerGroup != null);
                if (isGroupAvailableForVertex(producerGroup, executionVertexId)) {
                    return producerGroup;
                }
            }

            return null;
        }

        private boolean inSameLogicalSlotSharingGroup(
                final ExecutionVertexID executionVertexId1,
                final ExecutionVertexID executionVertexId2) {

            return Objects.equals(
                    getSlotSharingGroupId(executionVertexId1),
                    getSlotSharingGroupId(executionVertexId2));
        }

        private SlotSharingGroupId getSlotSharingGroupId(
                final ExecutionVertexID executionVertexId) {
            // slot sharing group of a vertex would never be null in production
            return checkNotNull(slotSharingGroupMap.get(executionVertexId.getJobVertexId()));
        }

        private boolean isGroupAvailableForVertex(
                final ExecutionSlotSharingGroup executionSlotSharingGroup,
                final ExecutionVertexID executionVertexId) {

            final Set<JobVertexID> assignedVertices =
                    assignedJobVerticesForGroups.get(executionSlotSharingGroup);
            return assignedVertices == null
                    || !assignedVertices.contains(executionVertexId.getJobVertexId());
        }

        private void addVertexToExecutionSlotSharingGroup(
                final SchedulingExecutionVertex vertex, final ExecutionSlotSharingGroup group) {

            group.addVertex(vertex.getId());
            executionSlotSharingGroupMap.put(vertex.getId(), group);
            assignedJobVerticesForGroups
                    .computeIfAbsent(group, k -> new HashSet<>())
                    .add(vertex.getId().getJobVertexId());
        }

        private void findAvailableOrCreateNewExecutionSlotSharingGroupFor(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                final SlotSharingGroupId slotSharingGroupId =
                        getSlotSharingGroupId(executionVertex.getId());
                final List<ExecutionSlotSharingGroup> groups =
                        executionSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupId, k -> new ArrayList<>());

                ExecutionSlotSharingGroup group = null;
                for (ExecutionSlotSharingGroup executionSlotSharingGroup : groups) {
                    if (isGroupAvailableForVertex(
                            executionSlotSharingGroup, executionVertex.getId())) {
                        group = executionSlotSharingGroup;
                        break;
                    }
                }

                if (group == null) {
                    group = new ExecutionSlotSharingGroup();
                    groups.add(group);
                }

                addVertexToExecutionSlotSharingGroup(executionVertex, group);
            }
        }

        private void updateConstraintToExecutionSlotSharingGroupMap(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                final ExecutionVertexID executionVertexId = executionVertex.getId();
                final CoLocationGroupDesc coLocationGroup =
                        coLocationGroupMap.get(executionVertexId.getJobVertexId());
                if (coLocationGroup != null) {
                    final CoLocationConstraintDesc constraint =
                            coLocationGroup.getLocationConstraint(
                                    executionVertexId.getSubtaskIndex());

                    constraintToExecutionSlotSharingGroupMap.put(
                            constraint, executionSlotSharingGroupMap.get(executionVertexId));
                }
            }
        }
    }
}
