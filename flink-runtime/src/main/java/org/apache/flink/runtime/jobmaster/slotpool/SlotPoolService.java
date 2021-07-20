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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

/** Service used by the {@link JobMaster} to manage a slot pool. */
public interface SlotPoolService extends AutoCloseable {

    /**
     * Tries to cast this slot pool service into the given clazz.
     *
     * @param clazz to cast the slot pool service into
     * @param <T> type of clazz
     * @return {@link Optional#of} the target type if it can be cast; otherwise {@link
     *     Optional#empty()}
     */
    default <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Start the encapsulated slot pool implementation.
     *
     * @param jobMasterId jobMasterId to start the service with
     * @param address address of the owner
     * @param mainThreadExecutor mainThreadExecutor to run actions in the main thread
     * @throws Exception if the the service cannot be started
     */
    void start(
            JobMasterId jobMasterId, String address, ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception;

    /** Close the slot pool service. */
    void close();

    /**
     * Offers multiple slots to the {@link SlotPoolService}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPoolService}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    /**
     * Fails the allocation with the given allocationId.
     *
     * @param taskManagerId taskManagerId is non-null if the signal comes from a TaskManager; if the
     *     signal comes from the ResourceManager, then it is null
     * @param allocationId allocationId identifies which allocation to fail
     * @param cause cause why the allocation failed
     * @return Optional task executor if it has no more slots registered
     */
    Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause);

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPoolService}.
     *
     * @param taskManagerId identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    boolean registerTaskManager(ResourceID taskManagerId);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPoolService}.
     *
     * @param taskManagerId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    boolean releaseTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     *
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    void disconnectResourceManager();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements.
     *
     * @param acquiredResources the resources that have been acquired
     */
    default void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {}
}
