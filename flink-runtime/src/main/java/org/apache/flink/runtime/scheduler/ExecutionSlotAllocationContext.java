/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.Optional;
import java.util.Set;

/** Context for slot allocation. */
interface ExecutionSlotAllocationContext extends InputsLocationsRetriever, StateLocationRetriever {

    /**
     * Returns required resources for an execution vertex.
     *
     * @param executionVertexId id of the execution vertex
     * @return required resources for the given execution vertex
     */
    ResourceProfile getResourceProfile(ExecutionVertexID executionVertexId);

    /**
     * Returns prior allocation id for an execution vertex.
     *
     * @param executionVertexId id of the execution vertex
     * @return prior allocation id for the given execution vertex if it exists; otherwise {@code
     *     Optional.empty()}
     */
    Optional<AllocationID> findPriorAllocationId(ExecutionVertexID executionVertexId);

    /**
     * Returns the scheduling topology containing all execution vertices and edges.
     *
     * @return scheduling topology
     */
    SchedulingTopology getSchedulingTopology();

    /**
     * Returns all slot sharing groups in the job.
     *
     * @return all slot sharing groups in the job
     */
    Set<SlotSharingGroup> getLogicalSlotSharingGroups();

    /**
     * Returns all co-location groups in the job.
     *
     * @return all co-location groups in the job
     */
    Set<CoLocationGroup> getCoLocationGroups();

    /**
     * Returns all reserved allocations. These allocations/slots were used to run certain vertices
     * and reserving them can prevent other vertices to take these slots and thus help vertices to
     * be deployed into their previous slots again after failover. It is needed if {@link
     * org.apache.flink.configuration.StateRecoveryOptions#LOCAL_RECOVERY} is enabled.
     *
     * @return all reserved allocations
     */
    Set<AllocationID> getReservedAllocations();
}
