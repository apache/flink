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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Strategy for allocating slots and task managers to fulfill the unfulfilled requirements. */
public interface ResourceAllocationStrategy {

    /**
     * Try to make an allocation decision to fulfill the resource requirements. The strategy
     * generates a series of actions to take, based on the current status.
     *
     * <p>Notice: For performance considerations, modifications might be performed directly on the
     * input arguments. If the arguments are reused elsewhere, please make a deep copy in advance.
     *
     * @param missingResources resource requirements that are not yet fulfilled, indexed by jobId
     * @param registeredResources tuples of available and default slot resource for registered task
     *     managers, indexed by instanceId
     * @param pendingTaskManagers available and default slot resources of pending task managers
     * @return a {@link ResourceAllocationResult} based on the current status, which contains
     *     whether the requirements can be fulfilled and the actions to take
     */
    ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            List<PendingTaskManager> pendingTaskManagers);
}
