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
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Base abstract implementation for {@link SlotSharingStrategy}. */
abstract class AbstractSlotSharingStrategy
        implements SlotSharingStrategy, SchedulingTopologyListener {

    protected final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    protected final Set<SlotSharingGroup> logicalSlotSharingGroups;

    protected final Set<CoLocationGroup> coLocationGroups;

    AbstractSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {
        this.logicalSlotSharingGroups = checkNotNull(slotSharingGroups);
        this.coLocationGroups = checkNotNull(coLocationGroups);

        this.executionSlotSharingGroupMap = computeExecutionSlotSharingGroups(topology);
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
                computeExecutionSlotSharingGroups(schedulingTopology);

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

    /**
     * The vertices are topologically sorted since {@link DefaultExecutionTopology#getVertices} are
     * topologically sorted.
     */
    @Nonnull
    static LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> getExecutionVertices(
            SchedulingTopology topology) {
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

    protected abstract Map<ExecutionVertexID, ExecutionSlotSharingGroup>
            computeExecutionSlotSharingGroups(SchedulingTopology schedulingTopology);
}
