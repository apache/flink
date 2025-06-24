/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Syncer for slot status. Take the responsibility of allocating/freeing slot and reconciling the
 * slot status with task managers.
 */
public interface SlotStatusSyncer {

    /**
     * Initialize this syncer.
     *
     * @param taskManagerTracker track the state of task managers and slots
     * @param resourceTracker track the state of resource declaration
     * @param resourceManagerId for slot allocation
     * @param mainThreadExecutor to handle the request future
     */
    void initialize(
            TaskManagerTracker taskManagerTracker,
            ResourceTracker resourceTracker,
            ResourceManagerId resourceManagerId,
            Executor mainThreadExecutor);

    /** Close this syncer, clear all the state. */
    void close();

    /**
     * Allocate a slot from the task manager.
     *
     * @param instanceId of the task manager
     * @param jobId of the slot
     * @param targetAddress of the job
     * @param resourceProfile of the slot
     * @return a {@link CompletableFuture} of the slot allocation, which will be completed
     *     exceptionally if the allocation fails
     */
    CompletableFuture<Void> allocateSlot(
            InstanceID instanceId,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile);

    /**
     * Free the given slot.
     *
     * @param allocationId of the given slot.
     */
    void freeSlot(AllocationID allocationId);

    /**
     * Reconcile the slot status with the slot report.
     *
     * @param instanceId of the task manager
     * @param slotReport reported
     * @return whether the previous allocations can be applied
     */
    boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);

    /**
     * Frees all currently inactive slot allocated for the given job.
     *
     * @param jobId of the job
     */
    void freeInactiveSlots(JobID jobId);
}
