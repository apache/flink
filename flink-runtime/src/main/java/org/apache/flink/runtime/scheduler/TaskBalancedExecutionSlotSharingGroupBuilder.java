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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Execute slot sharing groups builder based on the balanced scheduling strategy. */
public class TaskBalancedExecutionSlotSharingGroupBuilder {

    public static final Logger LOG =
            LoggerFactory.getLogger(TaskBalancedExecutionSlotSharingGroupBuilder.class);

    private final Map<JobVertexID, List<ExecutionVertexID>> allVertices;

    private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

    /** Record the {@link ExecutionSlotSharingGroup}s for {@link SlotSharingGroup}s. */
    private final Map<SlotSharingGroup, List<ExecutionSlotSharingGroup>>
            paralleledExecutionSlotSharingGroupsMap;

    /**
     * Record the next round-robin {@link ExecutionSlotSharingGroup} index for {@link
     * SlotSharingGroup}s.
     */
    private final Map<SlotSharingGroup, Integer> slotSharingGroupIndexMap;

    private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    private final Map<JobVertexID, CoLocationGroup> coLocationGroupMap;

    private final Map<CoLocationConstraint, ExecutionSlotSharingGroup>
            constraintToExecutionSlotSharingGroupMap;

    public TaskBalancedExecutionSlotSharingGroupBuilder(
            final Map<JobVertexID, List<ExecutionVertexID>> allVertices,
            final Collection<SlotSharingGroup> slotSharingGroups,
            final Collection<CoLocationGroup> coLocationGroups) {
        this.allVertices = checkNotNull(allVertices);

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

    public Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {

        initParalleledExecutionSlotSharingGroupsMap(allVertices);

        // Loop on job vertices
        for (Map.Entry<JobVertexID, List<ExecutionVertexID>> executionVertexInfos :
                allVertices.entrySet()) {

            JobVertexID jobVertexID = executionVertexInfos.getKey();
            List<ExecutionVertexID> executionVertexIds = executionVertexInfos.getValue();
            final SlotSharingGroup slotSharingGroup = slotSharingGroupMap.get(jobVertexID);

            if (!coLocationGroupMap.containsKey(jobVertexID)) {
                // For vertices without CoLocationConstraint.
                allocateNonCoLocatedVertices(slotSharingGroup, executionVertexIds);
            } else {
                // For vertices with CoLocationConstraint.
                allocateCoLocatedVertices(slotSharingGroup, executionVertexIds);
            }
        }
        return executionSlotSharingGroupMap;
    }

    private void initParalleledExecutionSlotSharingGroupsMap(
            final Map<JobVertexID, List<ExecutionVertexID>> allVertices) {

        allVertices.entrySet().stream()
                .map(
                        jobVertexExecutionVertices ->
                                Tuple2.of(
                                        slotSharingGroupMap.get(
                                                jobVertexExecutionVertices.getKey()),
                                        jobVertexExecutionVertices.getValue().size()))
                .collect(
                        Collectors.groupingBy(
                                tuple -> tuple.f0, Collectors.summarizingInt(tuple -> tuple.f1)))
                .forEach(
                        (slotSharingGroup, statistics) -> {
                            int slotNum = statistics.getMax();
                            paralleledExecutionSlotSharingGroupsMap.put(
                                    slotSharingGroup,
                                    createExecutionSlotSharingGroups(slotSharingGroup, slotNum));
                        });
    }

    private List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            SlotSharingGroup slotSharingGroup, int slotNum) {
        final List<ExecutionSlotSharingGroup> executionSlotSharingGroups = new ArrayList<>(slotNum);
        for (int i = 0; i < slotNum; i++) {
            final ExecutionSlotSharingGroup executionSlotSharingGroup =
                    new ExecutionSlotSharingGroup(slotSharingGroup);
            executionSlotSharingGroups.add(i, executionSlotSharingGroup);
            LOG.debug(
                    "Create {}-th(st/nd) executionSlotSharingGroup {}.",
                    i,
                    executionSlotSharingGroup);
        }
        return executionSlotSharingGroups;
    }

    private void allocateCoLocatedVertices(
            SlotSharingGroup slotSharingGroup, List<ExecutionVertexID> executionVertexIds) {

        final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                paralleledExecutionSlotSharingGroupsMap.get(slotSharingGroup);
        for (ExecutionVertexID executionVertexId : executionVertexIds) {
            final CoLocationConstraint coLocationConstraint =
                    getCoLocationConstraint(executionVertexId);
            ExecutionSlotSharingGroup executionSlotSharingGroup =
                    constraintToExecutionSlotSharingGroupMap.get(coLocationConstraint);
            if (Objects.isNull(executionSlotSharingGroup)) {
                executionSlotSharingGroup =
                        executionSlotSharingGroups.get(
                                getLeastUtilizeSlotIndex(
                                        executionSlotSharingGroups, executionVertexId));
                constraintToExecutionSlotSharingGroupMap.put(
                        coLocationConstraint, executionSlotSharingGroup);
            }
            addVertexToExecutionSlotSharingGroup(executionSlotSharingGroup, executionVertexId);
        }
        final int jobVertexParallel = executionVertexIds.size();
        if (!isMaxParallelism(jobVertexParallel, slotSharingGroup)) {
            int index = getLeastUtilizeSlotIndex(executionSlotSharingGroups, null);
            updateSlotRoundRobinIndexIfNeeded(jobVertexParallel, slotSharingGroup, index);
        }
    }

    private void allocateNonCoLocatedVertices(
            SlotSharingGroup slotSharingGroup, List<ExecutionVertexID> executionVertices) {
        final int jobVertexParallel = executionVertices.size();
        int index = getSlotRoundRobinIndex(jobVertexParallel, slotSharingGroup);
        final List<ExecutionSlotSharingGroup> executionSlotSharingGroups =
                paralleledExecutionSlotSharingGroupsMap.get(slotSharingGroup);
        for (ExecutionVertexID executionVertexId : executionVertices) {
            addVertexToExecutionSlotSharingGroup(
                    executionSlotSharingGroups.get(index), executionVertexId);
            index = ++index % executionSlotSharingGroups.size();
        }
        updateSlotRoundRobinIndexIfNeeded(executionVertices.size(), slotSharingGroup, index);
    }

    private void addVertexToExecutionSlotSharingGroup(
            ExecutionSlotSharingGroup executionSlotSharingGroup,
            ExecutionVertexID executionVertexId) {
        executionSlotSharingGroup.addVertex(executionVertexId);
        executionSlotSharingGroupMap.put(executionVertexId, executionSlotSharingGroup);
    }

    private CoLocationConstraint getCoLocationConstraint(ExecutionVertexID executionVertexId) {
        final JobVertexID jobVertexID = executionVertexId.getJobVertexId();
        final int subtaskIndex = executionVertexId.getSubtaskIndex();
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
            @Nullable final ExecutionVertexID executionVertexId) {
        int indexWithLeastExecutionVertices = 0;
        int leastExecutionVertices = Integer.MAX_VALUE;
        for (int index = 0; index < executionSlotSharingGroups.size(); index++) {
            final ExecutionSlotSharingGroup executionSlotSharingGroup =
                    executionSlotSharingGroups.get(index);
            final int executionVertices = executionSlotSharingGroup.getExecutionVertexIds().size();
            if (leastExecutionVertices > executionVertices
                    && (Objects.isNull(executionVertexId)
                            || allocatable(executionSlotSharingGroup, executionVertexId))) {
                indexWithLeastExecutionVertices = index;
                leastExecutionVertices = executionVertices;
            }
        }
        return indexWithLeastExecutionVertices;
    }

    private boolean allocatable(
            final ExecutionSlotSharingGroup executionSlotSharingGroup,
            @Nonnull ExecutionVertexID executionVertexId) {
        final JobVertexID jobVertexId = executionVertexId.getJobVertexId();
        final Set<JobVertexID> allocatedJobVertices =
                executionSlotSharingGroup.getExecutionVertexIds().stream()
                        .map(ExecutionVertexID::getJobVertexId)
                        .collect(Collectors.toSet());
        return !allocatedJobVertices.contains(jobVertexId);
    }
}
