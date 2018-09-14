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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * The gateway for calls on the {@link SlotPool}.
 */
public interface SlotPoolGateway extends AllocatedSlotActions, RpcGateway {

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	void suspend();

	// ------------------------------------------------------------------------
	//  resource manager connection
	// ------------------------------------------------------------------------

	/**
	 * Connects the SlotPool to the given ResourceManager. After this method is called, the
	 * SlotPool will be able to request resources from the given ResourceManager.
	 *
	 * @param resourceManagerGateway  The RPC gateway for the resource manager.
	 */
	void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

	/**
	 * Disconnects the slot pool from its current Resource Manager. After this call, the pool will not
	 * be able to request further slots from the Resource Manager, and all currently pending requests
	 * to the resource manager will be canceled.
	 *
	 * <p>The slot pool will still be able to serve slots from its internal pool.
	 */
	void disconnectResourceManager();

	// ------------------------------------------------------------------------
	//  registering / un-registering TaskManagers and slots
	// ------------------------------------------------------------------------

	/**
	 * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPool}.
	 *
	 * @param resourceID identifying the TaskExecutor to register
	 * @return Future acknowledge which is completed after the TaskExecutor has been registered
	 */
	CompletableFuture<Acknowledge> registerTaskManager(ResourceID resourceID);

	/**
	 * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPool}.
	 *
	 * @param resourceId identifying the TaskExecutor which shall be released from the SlotPool
	 * @param cause for the releasing of the TaskManager
	 * @return Future acknowledge which is completed after the TaskExecutor has been released
	 */
	CompletableFuture<Acknowledge> releaseTaskManager(final ResourceID resourceId, final Exception cause);

	/**
	 * Offers a slot to the {@link SlotPool}. The slot offer can be accepted or
	 * rejected.
	 *
	 * @param taskManagerLocation from which the slot offer originates
	 * @param taskManagerGateway to talk to the slot offerer
	 * @param slotOffer slot which is offered to the {@link SlotPool}
	 * @return True (future) if the slot has been accepted, otherwise false (future)
	 */
	CompletableFuture<Boolean> offerSlot(
		TaskManagerLocation taskManagerLocation,
		TaskManagerGateway taskManagerGateway,
		SlotOffer slotOffer);

	/**
	 * Offers multiple slots to the {@link SlotPool}. The slot offerings can be
	 * individually accepted or rejected by returning the collection of accepted
	 * slot offers.
	 *
	 * @param taskManagerLocation from which the slot offers originate
	 * @param taskManagerGateway to talk to the slot offerer
	 * @param offers slot offers which are offered to the {@link SlotPool}
	 * @return A collection of accepted slot offers (future). The remaining slot offers are
	 * 			implicitly rejected.
	 */
	CompletableFuture<Collection<SlotOffer>> offerSlots(
		TaskManagerLocation taskManagerLocation,
		TaskManagerGateway taskManagerGateway,
		Collection<SlotOffer> offers);

	/**
	 * Fails the slot with the given allocation id.
	 *
	 * @param allocationID identifying the slot which is being failed
	 * @param cause of the failure
	 */
	void failAllocation(AllocationID allocationID, Exception cause);

	// ------------------------------------------------------------------------
	//  allocating and disposing slots
	// ------------------------------------------------------------------------

	/**
	 * Requests to allocate a slot for the given {@link ScheduledUnit}. The request
	 * is uniquely identified by the provided {@link SlotRequestId} which can also
	 * be used to release the slot via {@link #releaseSlot(SlotRequestId, SlotSharingGroupId, Throwable)}.
	 * The allocated slot will fulfill the requested {@link ResourceProfile} and it
	 * is tried to place it on one of the location preferences.
	 *
	 * <p>If the returned future must not be completed right away (a.k.a. the slot request
	 * can be queued), allowQueuedScheduling must be set to true.
	 *
	 * @param slotRequestId identifying the requested slot
	 * @param scheduledUnit for which to allocate slot
	 * @param slotProfile profile that specifies the requirements for the requested slot
	 * @param allowQueuedScheduling true if the slot request can be queued (e.g. the returned future must not be completed)
	 * @param timeout for the operation
	 * @return Future which is completed with the allocated {@link LogicalSlot}
	 */
	CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			@RpcTimeout Time timeout);
}
