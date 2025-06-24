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
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;
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
     * @param taskManagerResourceInfoProvider provide the registered/pending resources of the
     *     current cluster
     * @param blockedTaskManagerChecker blocked task manager checker
     * @return a {@link ResourceAllocationResult} based on the current status, which contains
     *     whether the requirements can be fulfilled and the actions to take
     */
    ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker);

    /**
     * Try to make a decision to reconcile the cluster resources. This is more light weighted than
     * {@link #tryFulfillRequirements}, only consider empty registered / pending workers and assume
     * all requirements are fulfilled by registered / pending workers.
     *
     * @param taskManagerResourceInfoProvider provide the registered/pending resources of the
     *     current cluster
     * @return a {@link ResourceReconcileResult} based on the current status, which contains the
     *     actions to take
     */
    ResourceReconcileResult tryReconcileClusterResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider);
}
