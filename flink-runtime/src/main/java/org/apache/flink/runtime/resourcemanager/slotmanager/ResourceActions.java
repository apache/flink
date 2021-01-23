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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.Collection;

/** Resource related actions which the {@link SlotManager} can perform. */
public interface ResourceActions {

    /**
     * Releases the resource with the given instance id.
     *
     * @param instanceId identifying which resource to release
     * @param cause why the resource is released
     */
    void releaseResource(InstanceID instanceId, Exception cause);

    /**
     * Requests to allocate a resource with the given {@link WorkerResourceSpec}.
     *
     * @param workerResourceSpec for the to be allocated worker
     * @return whether the resource can be allocated
     */
    boolean allocateResource(WorkerResourceSpec workerResourceSpec);

    /**
     * Notifies that an allocation failure has occurred.
     *
     * @param jobId to which the allocation belonged
     * @param allocationId identifying the failed allocation
     * @param cause of the allocation failure
     */
    void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements of a
     * job.
     *
     * @param jobId job for which not enough resources are available
     * @param acquiredResources the resources that have been acquired for the job
     */
    void notifyNotEnoughResourcesAvailable(
            JobID jobId, Collection<ResourceRequirement> acquiredResources);
}
