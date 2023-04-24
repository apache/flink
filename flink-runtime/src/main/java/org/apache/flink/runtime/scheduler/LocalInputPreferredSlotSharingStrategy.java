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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
class LocalInputPreferredSlotSharingStrategy
        implements SlotSharingStrategy, SchedulingTopologyListener {

    private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    private final Set<SlotSharingGroup> logicalSlotSharingGroups;

    private final Set<CoLocationGroup> coLocationGroups;

    LocalInputPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> logicalSlotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {

        this.logicalSlotSharingGroups = checkNotNull(logicalSlotSharingGroups);
        this.coLocationGroups = checkNotNull(coLocationGroups);

        this.executionSlotSharingGroupMap =
                new ExecutionSlotSharingGroupBuilder(
                                topology, logicalSlotSharingGroups, coLocationGroups)
                        .build();
        topology.registerSchedulingTopologyListener(this);
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

    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {

        final Map<ExecutionVertexID, ExecutionSlotSharingGroup> newMap =
                new LocalInputPreferredSlotSharingStrategy.ExecutionSlotSharingGroupBuilder(
                                schedulingTopology, logicalSlotSharingGroups, coLocationGroups)
                        .build();

        for (ExecutionVertexID vertexId : newMap.keySet()) {
            final ExecutionSlotSharingGroup newEssg = newMap.get(vertexId);
            final ExecutionSlotSharingGroup oldEssg = executionSlotSharingGroupMap.get(vertexId);
            if (oldEssg == null) {
                executionSlotSharingGroupMap.put(vertexId, newEssg);
            } else {
                // ensures that existing slot sharing groups are not changed
                checkState(
                        oldEssg.getExecutionVertexIds().equals(newEssg.getExecutionVertexIds()),
                        "Existing ExecutionSlotSharingGroups are changed after topology update");
            }
        }
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public LocalInputPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new LocalInputPreferredSlotSharingStrategy(
                    topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    private static class ExecutionSlotSharingGroupBuilder {
        private final SchedulingTopology topology;

        private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

        private final Map<JobVertexID, CoLocationGroup> coLocationGroupMap;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        private final Map<CoLocationConstraint, ExecutionSlotSharingGroup>
                constraintToExecutionSlotSharingGroupMap;

        /**
         * A JobVertex only belongs to one {@link SlotSharingGroup}. A SlotSharingGroup is
         * corresponding to a set of {@link ExecutionSlotSharingGroup}s. We can maintain available
         * ExecutionSlotSharingGroups for each JobVertex.
         *
         * <p>Once an ExecutionSlotSharingGroup is created, it becomes available for all JobVertices
         * in the corresponding SlotSharingGroup in the beginning.
         *
         * <p>Once a SchedulingExecutionVertex is added to the ExecutionSlotSharingGroup, the group
         * is no longer available for other SchedulingExecutionVertices with the same JobVertexID.
         *
         * <p>Here we use {@link LinkedHashSet} to reserve the order the same as the
         * SchedulingVertices are traversed.
         */
        private final Map<JobVertexID, LinkedHashSet<ExecutionSlotSharingGroup>>
                availableGroupsForJobVertex;

        /**
         * Maintains the candidate {@link ExecutionSlotSharingGroup}s for every {@link
         * ConsumedPartitionGroup}. The ConsumedPartitionGroup represents a group of partitions that
         * is consumed by the same ExecutionVertices. These ExecutionVertices belong to one consumer
         * JobVertex. Thus, we can say, a ConsumedPartitionGroup is corresponding to one consumer
         * JobVertex.
         *
         * <p>This mapping is used to find an available producer ExecutionSlotSharingGroup for the
         * consumer vertex. If a candidate group is available for this consumer vertex, it will be
         * assigned to this vertex.
         *
         * <p>The candidate groups are computed in {@link
         * #computeAllCandidateGroupsForConsumedPartitionGroup} when the ConsumedPartitionGroup is
         * traversed for the first time.
         *
         * <p>Here we use {@link LinkedHashSet} to reserve the order the same as the
         * SchedulingVertices are traversed.
         */
        private final Map<ConsumedPartitionGroup, LinkedHashSet<ExecutionSlotSharingGroup>>
                candidateGroupsForConsumedPartitionGroup;

        private ExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            this.topology = checkNotNull(topology);

            this.slotSharingGroupMap = new HashMap<>();
            for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup);
                }
            }

            this.coLocationGroupMap = new HashMap<>();
            for (CoLocationGroup coLocationGroup : coLocationGroups) {
                for (JobVertexID jobVertexId : coLocationGroup.getVertexIds()) {
                    coLocationGroupMap.put(jobVertexId, coLocationGroup);
                }
            }

            executionSlotSharingGroupMap = new HashMap<>();
            constraintToExecutionSlotSharingGroupMap = new HashMap<>();
            availableGroupsForJobVertex = new HashMap<>();
            candidateGroupsForConsumedPartitionGroup = new IdentityHashMap<>();
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

        /**
         * The vertices are topologically sorted since {@link DefaultExecutionTopology#getVertices}
         * are topologically sorted.
         */
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
            final CoLocationGroup coLocationGroup =
                    coLocationGroupMap.get(executionVertexId.getJobVertexId());
            if (coLocationGroup != null) {
                final CoLocationConstraint constraint =
                        coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());

                return constraintToExecutionSlotSharingGroupMap.get(constraint);
            } else {
                return null;
            }
        }

        private ExecutionSlotSharingGroup tryFindAvailableProducerExecutionSlotSharingGroupFor(
                final SchedulingExecutionVertex executionVertex) {

            final ExecutionVertexID executionVertexId = executionVertex.getId();

            for (ConsumedPartitionGroup consumedPartitionGroup :
                    executionVertex.getConsumedPartitionGroups()) {

                Set<ExecutionSlotSharingGroup> candidateGroups =
                        candidateGroupsForConsumedPartitionGroup.computeIfAbsent(
                                consumedPartitionGroup,
                                group ->
                                        computeAllCandidateGroupsForConsumedPartitionGroup(
                                                executionVertexId.getJobVertexId(), group));

                Iterator<ExecutionSlotSharingGroup> candidateIterator = candidateGroups.iterator();

                while (candidateIterator.hasNext()) {
                    ExecutionSlotSharingGroup candidateGroup = candidateIterator.next();
                    // There are two cases for this candidate group:
                    //
                    // 1. The group is available for this vertex, and it will be assigned to this
                    // vertex;
                    // 2. The group is not available for this vertex, because it's already assigned
                    // to another vertex with the same JobVertexID.
                    //
                    // No matter what case it is, the candidate group is no longer a candidate and
                    // should be removed.
                    candidateIterator.remove();
                    if (isExecutionSlotSharingGroupAvailableForVertex(
                            candidateGroup, executionVertexId)) {
                        return candidateGroup;
                    }
                }
            }

            return null;
        }

        private boolean isExecutionSlotSharingGroupAvailableForVertex(
                ExecutionSlotSharingGroup executionSlotSharingGroup, ExecutionVertexID vertexId) {

            Set<ExecutionSlotSharingGroup> availableGroupsForCurrentVertex =
                    availableGroupsForJobVertex.get(vertexId.getJobVertexId());

            return availableGroupsForCurrentVertex != null
                    && availableGroupsForCurrentVertex.contains(executionSlotSharingGroup);
        }

        private boolean inSameLogicalSlotSharingGroup(
                final JobVertexID jobVertexId1, final JobVertexID jobVertexId2) {

            return Objects.equals(
                    getSlotSharingGroup(jobVertexId1).getSlotSharingGroupId(),
                    getSlotSharingGroup(jobVertexId2).getSlotSharingGroupId());
        }

        private SlotSharingGroup getSlotSharingGroup(final JobVertexID jobVertexId) {
            // slot sharing group of a vertex would never be null in production
            return checkNotNull(slotSharingGroupMap.get(jobVertexId));
        }

        private void addVertexToExecutionSlotSharingGroup(
                final SchedulingExecutionVertex vertex, final ExecutionSlotSharingGroup group) {

            ExecutionVertexID executionVertexId = vertex.getId();
            group.addVertex(executionVertexId);
            executionSlotSharingGroupMap.put(executionVertexId, group);

            // The ExecutionSlotSharingGroup is no longer available for the JobVertex
            Set<ExecutionSlotSharingGroup> availableExecutionSlotSharingGroups =
                    availableGroupsForJobVertex.get(executionVertexId.getJobVertexId());
            if (availableExecutionSlotSharingGroups != null) {
                availableExecutionSlotSharingGroups.remove(group);
            }
        }

        private void findAvailableOrCreateNewExecutionSlotSharingGroupFor(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {

                ExecutionSlotSharingGroup group =
                        tryFindAvailableExecutionSlotSharingGroupFor(executionVertex);

                if (group == null) {
                    group = createNewExecutionSlotSharingGroup(executionVertex.getId());
                }

                addVertexToExecutionSlotSharingGroup(executionVertex, group);
            }
        }

        private ExecutionSlotSharingGroup tryFindAvailableExecutionSlotSharingGroupFor(
                SchedulingExecutionVertex executionVertex) {

            Set<ExecutionSlotSharingGroup> availableGroupsForCurrentVertex =
                    availableGroupsForJobVertex.get(executionVertex.getId().getJobVertexId());

            if (availableGroupsForCurrentVertex != null
                    && !availableGroupsForCurrentVertex.isEmpty()) {
                return availableGroupsForCurrentVertex.iterator().next();
            }

            return null;
        }

        private ExecutionSlotSharingGroup createNewExecutionSlotSharingGroup(
                ExecutionVertexID executionVertexId) {
            final SlotSharingGroup slotSharingGroup =
                    getSlotSharingGroup(executionVertexId.getJobVertexId());

            final ExecutionSlotSharingGroup newGroup = new ExecutionSlotSharingGroup();
            newGroup.setResourceProfile(slotSharingGroup.getResourceProfile());

            // Once a new ExecutionSlotSharingGroup is created, it's available for all JobVertices
            // in this SlotSharingGroup
            for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                Set<ExecutionSlotSharingGroup> availableExecutionSlotSharingGroups =
                        availableGroupsForJobVertex.computeIfAbsent(
                                jobVertexId, ignore -> new LinkedHashSet<>());
                availableExecutionSlotSharingGroups.add(newGroup);
            }

            return newGroup;
        }

        private void updateConstraintToExecutionSlotSharingGroupMap(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                final ExecutionVertexID executionVertexId = executionVertex.getId();
                final CoLocationGroup coLocationGroup =
                        coLocationGroupMap.get(executionVertexId.getJobVertexId());
                if (coLocationGroup != null) {
                    final CoLocationConstraint constraint =
                            coLocationGroup.getLocationConstraint(
                                    executionVertexId.getSubtaskIndex());

                    constraintToExecutionSlotSharingGroupMap.put(
                            constraint, executionSlotSharingGroupMap.get(executionVertexId));
                }
            }
        }

        private LinkedHashSet<ExecutionSlotSharingGroup>
                computeAllCandidateGroupsForConsumedPartitionGroup(
                        JobVertexID consumerJobVertexId,
                        ConsumedPartitionGroup consumedPartitionGroup) {

            // We tend to reserve the order of ExecutionSlotSharingGroups as they are traversed
            // topologically
            final LinkedHashSet<ExecutionSlotSharingGroup> candidateExecutionSlotSharingGroups =
                    new LinkedHashSet<>();

            JobVertexID producerJobVertexId =
                    topology.getResultPartition(consumedPartitionGroup.getFirst())
                            .getProducer()
                            .getId()
                            .getJobVertexId();

            // Check if the producer JobVertex and the consumer JobVertex are in the same
            // SlotSharingGroup
            if (inSameLogicalSlotSharingGroup(producerJobVertexId, consumerJobVertexId)) {

                // Iterate over the producer ExecutionVertices of all the partitions in the
                // ConsumedPartitionGroup
                for (IntermediateResultPartitionID consumedPartition : consumedPartitionGroup) {

                    ExecutionVertexID producerExecutionVertexId =
                            topology.getResultPartition(consumedPartition).getProducer().getId();

                    ExecutionSlotSharingGroup assignedGroupForProducerExecutionVertex =
                            executionSlotSharingGroupMap.get(producerExecutionVertexId);
                    checkNotNull(assignedGroupForProducerExecutionVertex);

                    candidateExecutionSlotSharingGroups.add(
                            assignedGroupForProducerExecutionVertex);
                }
            }

            return candidateExecutionSlotSharingGroups;
        }
    }
}
