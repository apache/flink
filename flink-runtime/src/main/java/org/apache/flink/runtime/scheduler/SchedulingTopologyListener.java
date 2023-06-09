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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** This listener will be notified whenever the scheduling topology is updated. */
public interface SchedulingTopologyListener {

    /**
     * Notifies that the scheduling topology is just updated.
     *
     * @param schedulingTopology the scheduling topology which is just updated
     * @param newExecutionVertices the newly added execution vertices.
     */
    void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices);

    static void updateExecutionSlotSharingGroupMapIfNeeded(
            Map<ExecutionVertexID, ExecutionSlotSharingGroup> newMap,
            Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap) {
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
}
