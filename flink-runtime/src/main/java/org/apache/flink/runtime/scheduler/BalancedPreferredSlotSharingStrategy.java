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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This strategy tries to get a balanced tasks scheduling. Execution vertices, which are belong to
 * the same SlotSharingGroup, tend to be put in the same ExecutionSlotSharingGroup. Co-location
 * constraints are ignored at present.
 */
public class BalancedPreferredSlotSharingStrategy
        implements SlotSharingStrategy, SchedulingTopologyListener {

    private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    private final @Nonnull Set<SlotSharingGroup> logicalSlotSharingGroups;

    private @Nonnull Map<ExecutionSlotSharingGroup, SlotSharingGroup> executionSSGTologicalSSGMap;

    public BalancedPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> logicalSlotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {
        this.logicalSlotSharingGroups = checkNotNull(logicalSlotSharingGroups);

        BalancedExecutionSlotSharingGroupBuilder builder =
                new BalancedExecutionSlotSharingGroupBuilder(topology, logicalSlotSharingGroups);

        this.executionSlotSharingGroupMap = builder.build();
        this.executionSSGTologicalSSGMap = builder.getSlotSharingGroupMap();

        topology.registerSchedulingTopologyListener(this);
    }

    @Override
    public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
            ExecutionVertexID executionVertexId) {
        return executionSlotSharingGroupMap.get(executionVertexId);
    }

    @Override
    public SlotSharingGroup getSlotSharingGroup(
            ExecutionSlotSharingGroup executionSlotSharingGroup) {
        return executionSSGTologicalSSGMap.get(executionSlotSharingGroup);
    }

    @Override
    public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
        return new HashSet<>(executionSlotSharingGroupMap.values());
    }

    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {
        BalancedExecutionSlotSharingGroupBuilder builder =
                new BalancedExecutionSlotSharingGroupBuilder(
                        schedulingTopology, logicalSlotSharingGroups);
        final Map<ExecutionVertexID, ExecutionSlotSharingGroup> newMap = builder.build();
        executionSSGTologicalSSGMap = builder.getSlotSharingGroupMap();

        SchedulingTopologyListener.updateExecutionSlotSharingGroupMapIfNeeded(
                newMap, executionSlotSharingGroupMap);
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public BalancedPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new BalancedPreferredSlotSharingStrategy(
                    topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    private static class BalancedExecutionSlotSharingGroupBuilder {

        private final SchedulingTopology topology;

        private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

        /** Record the parallelism of a {@link org.apache.flink.runtime.jobgraph.JobVertex}. */
        private final Map<JobVertexID, Integer> jVIDToParallelismMap;

        /**
         * Record the number of {@link ExecutionSlotSharingGroup}s for {@link SlotSharingGroup}s.
         */
        private final Map<SlotSharingGroup, List<ExecutionSlotSharingGroup>>
                logicalSSGToExecutionSSGsMap;

        /**
         * Record the next round-robin {@link ExecutionSlotSharingGroup} index for {@link
         * SlotSharingGroup}s.
         */
        private final Map<SlotSharingGroup, Integer> logicalSSGToNextExecutionSSGIndex;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        private BalancedExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups) {

            this.topology = checkNotNull(topology);

            this.jVIDToParallelismMap = new HashMap<>();
            this.logicalSSGToExecutionSSGsMap = new HashMap<>(logicalSlotSharingGroups.size());
            this.logicalSSGToNextExecutionSSGIndex = new HashMap<>(logicalSlotSharingGroups.size());
            this.slotSharingGroupMap = new HashMap<>();
            this.executionSlotSharingGroupMap = new HashMap<>();

            for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup);
                }
                logicalSSGToNextExecutionSSGIndex.putIfAbsent(slotSharingGroup, 0);
            }
        }

        /** Get the map of {@link JobVertexID} to {@link SlotSharingGroup}. */
        Map<ExecutionSlotSharingGroup, SlotSharingGroup> getSlotSharingGroupMap() {
            Map<ExecutionSlotSharingGroup, SlotSharingGroup> result = new HashMap<>();
            logicalSSGToExecutionSSGsMap.forEach(
                    (ssg, eSSGs) -> eSSGs.forEach(eSSG -> result.put(eSSG, ssg)));
            return result;
        }

        private void computeSlotSharingGroupToExecutionSlotSharingGroupMap() {

            jVIDToParallelismMap.entrySet().stream()
                    .collect(
                            Collectors.groupingBy(
                                    jobVertexIDIntegerEntry ->
                                            slotSharingGroupMap.get(
                                                    jobVertexIDIntegerEntry.getKey())))
                    .forEach(
                            (k, v) -> {
                                Integer numberSlotOfSSG =
                                        v.stream()
                                                .map(Map.Entry::getValue)
                                                .max(Comparator.comparingInt(o -> o))
                                                .orElseThrow(
                                                        () ->
                                                                new FlinkRuntimeException(
                                                                        String.format(
                                                                                "Error state when computing the number of slots in %s",
                                                                                k)));
                                List<ExecutionSlotSharingGroup> eSSGs =
                                        new ArrayList<>(numberSlotOfSSG);
                                for (int i = 0; i < numberSlotOfSSG; i++) {
                                    ExecutionSlotSharingGroup executionSlotSharingGroup =
                                            new ExecutionSlotSharingGroup();
                                    executionSlotSharingGroup.setResourceProfile(
                                            k.getResourceProfile());
                                    eSSGs.add(i, executionSlotSharingGroup);
                                }
                                logicalSSGToExecutionSSGsMap.put(k, eSSGs);
                            });
        }

        private boolean isParallelismEqualsToESsgNumber(
                JobVertexID jobVertexID, SlotSharingGroup ssg) {
            return Objects.equals(
                    jVIDToParallelismMap.get(jobVertexID),
                    logicalSSGToExecutionSSGsMap.get(ssg).size());
        }

        private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices =
                    SlotSharingStrategy.getExecutionVertices(topology);

            allVertices.forEach((jvID, sEVs) -> jVIDToParallelismMap.put(jvID, sEVs.size()));
            computeSlotSharingGroupToExecutionSlotSharingGroupMap();

            // Loop on job vertices
            for (Map.Entry<JobVertexID, List<SchedulingExecutionVertex>> entry :
                    allVertices.entrySet()) {
                // If the parallelism of the jVID is equals to the number of
                // ExecutionSlotSharingGroups in the current logical ssg.
                SlotSharingGroup logicalSSG = slotSharingGroupMap.get(entry.getKey());
                List<ExecutionSlotSharingGroup> executionSSGs =
                        logicalSSGToExecutionSSGsMap.get(logicalSSG);

                int index =
                        isParallelismEqualsToESsgNumber(entry.getKey(), logicalSSG)
                                ? 0
                                : logicalSSGToNextExecutionSSGIndex.get(logicalSSG);

                for (SchedulingExecutionVertex sev : entry.getValue()) {
                    executionSSGs.get(index).addVertex(sev.getId());
                    executionSlotSharingGroupMap.put(sev.getId(), executionSSGs.get(index));
                    ++index;
                    if (!isParallelismEqualsToESsgNumber(entry.getKey(), logicalSSG)) {
                        index = index % executionSSGs.size();
                    }
                }
                if (!isParallelismEqualsToESsgNumber(entry.getKey(), logicalSSG)) {
                    logicalSSGToNextExecutionSSGIndex.put(logicalSSG, index);
                }
            }
            return executionSlotSharingGroupMap;
        }
    }
}
