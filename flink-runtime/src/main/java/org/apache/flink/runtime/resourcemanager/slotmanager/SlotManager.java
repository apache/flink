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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.concurrent.Executor;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or and
 * allocated slot is freed, then it tries to fulfill another pending slot request. Whenever there
 * are not enough slots available the slot manager will notify the resource manager about it via
 * {@link ResourceActions#allocateResource(ResourceProfile)}.
 *
 * <p>In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and pending slot requests time out triggering their release and
 * failure, respectively.
 */
public interface SlotManager extends AutoCloseable {
	int getNumberRegisteredSlots();

	int getNumberRegisteredSlotsOf(InstanceID instanceId);

	int getNumberFreeSlots();

	int getNumberFreeSlotsOf(InstanceID instanceId);

	int getNumberPendingTaskManagerSlots();

	int getNumberPendingSlotRequests();

	boolean isFailingUnfulfillableRequest();

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions);

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	void suspend();

	/**
	 * Requests a slot with the respective resource profile.
	 *
	 * @param slotRequest specifying the requested slot specs
	 * @return true if the slot request was registered; false if the request is a duplicate
	 * @throws SlotManagerException if the slot request failed (e.g. not enough resources left)
	 */
	boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException;

	/**
	 * Cancels and removes a pending slot request with the given allocation id. If there is no such
	 * pending request, then nothing is done.
	 *
	 * @param allocationId identifying the pending slot request
	 * @return True if a pending slot request was found; otherwise false
	 */
	boolean unregisterSlotRequest(AllocationID allocationId);

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 */
	void registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport);

	/**
	 * Unregisters the task manager identified by the given instance id and its associated slots
	 * from the slot manager.
	 *
	 * @param instanceId identifying the task manager to unregister
	 * @return True if there existed a registered task manager with the given instance id
	 */
	boolean unregisterTaskManager(InstanceID instanceId);

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 */
	boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	void freeSlot(SlotID slotId, AllocationID allocationId);

	void setFailUnfulfillableRequest(boolean failUnfulfillableRequest);

	@VisibleForTesting
	void unregisterTaskManagersAndReleaseResources();
}
