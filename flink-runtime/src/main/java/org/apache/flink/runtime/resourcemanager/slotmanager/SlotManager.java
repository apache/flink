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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServices;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.SlotRequestRegistered;
import org.apache.flink.runtime.resourcemanager.SlotRequestReply;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * SlotManager is responsible for receiving slot requests and do slot allocations. It allows to request
 * slots from registered TaskManagers and issues container allocation requests in case of there are not
 * enough available slots. Besides, it should sync its slot allocation with TaskManager's heartbeat.
 * <p>
 * The main operation principle of SlotManager is:
 * <ul>
 * <li>1. All slot allocation status should be synced with TaskManager, which is the ground truth.</li>
 * <li>2. All slots that have registered must be tracked, either by free pool or allocated pool.</li>
 * <li>3. All slot requests will be handled by best efforts, there is no guarantee that one request will be
 * fulfilled in time or correctly allocated. Conflicts or timeout or some special error will happen, it should
 * be handled outside SlotManager. SlotManager will make each decision based on the information it currently
 * holds.</li>
 * </ul>
 * <b>IMPORTANT:</b> This class is <b>Not Thread-safe</b>.
 */
public abstract class SlotManager {

	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	/** All registered task managers with ResourceID and gateway. */
	private final Map<ResourceID, TaskExecutorGateway> taskManagerGateways;

	/** All registered slots, including free and allocated slots */
	private final Map<ResourceID, Map<SlotID, ResourceSlot>> registeredSlots;

	/** All pending slot requests, waiting available slots to fulfil */
	private final Map<AllocationID, SlotRequest> pendingSlotRequests;

	/** All free slots that can be used to be allocated */
	private final Map<SlotID, ResourceSlot> freeSlots;

	/** All allocations, we can lookup allocations either by SlotID or AllocationID */
	private final AllocationMap allocationMap;

	private final Time timeout;

	/** The current leader id set by the ResourceManager */
	private UUID leaderID;

	/** The Resource allocation provider */
	private ResourceManagerServices resourceManagerServices;

	public SlotManager() {
		this.registeredSlots = new HashMap<>(16);
		this.pendingSlotRequests = new LinkedHashMap<>(16);
		this.freeSlots = new HashMap<>(16);
		this.allocationMap = new AllocationMap();
		this.taskManagerGateways = new HashMap<>();
		this.timeout = Time.seconds(10);
	}

	/**
	 * Initializes the resource supplier which is needed to request new resources.
	 */
	public void setupResourceManagerServices(ResourceManagerServices resourceManagerServices) {
		if (this.resourceManagerServices != null) {
			throw new IllegalStateException("ResourceManagerServices may only be set once.");
		}
		this.resourceManagerServices = resourceManagerServices;
	}


	// ------------------------------------------------------------------------
	//  slot managements
	// ------------------------------------------------------------------------

	/**
	 * Request a slot with requirements, we may either fulfill the request or pending it. Trigger container
	 * allocation if we don't have enough resource. If we have free slot which can match the request, record
	 * this allocation and forward the request to TaskManager through ResourceManager (we want this done by
	 * RPC's main thread to avoid race condition).
	 *
	 * @param request The detailed request of the slot
	 * @return SlotRequestRegistered The confirmation message to be send to the caller
	 */
	public SlotRequestRegistered requestSlot(final SlotRequest request) {
		final AllocationID allocationId = request.getAllocationId();
		if (isRequestDuplicated(request)) {
			LOG.warn("Duplicated slot request, AllocationID:{}", allocationId);
			return null;
		}

		// try to fulfil the request with current free slots
		final ResourceSlot slot = chooseSlotToUse(request, freeSlots);
		if (slot != null) {
			LOG.info("Assigning SlotID({}) to AllocationID({}), JobID:{}", slot.getSlotId(),
				allocationId, request.getJobId());

			// record this allocation in bookkeeping
			allocationMap.addAllocation(slot.getSlotId(), allocationId);
			// remove selected slot from free pool
			final ResourceSlot removedSlot = freeSlots.remove(slot.getSlotId());

			final Future<SlotRequestReply> slotRequestReplyFuture =
				slot.getTaskExecutorGateway().requestSlot(allocationId, leaderID, timeout);

			slotRequestReplyFuture.handleAsync(new BiFunction<SlotRequestReply, Throwable, Object>() {
				@Override
				public Object apply(SlotRequestReply slotRequestReply, Throwable throwable) {
					if (throwable != null) {
						// we failed, put the slot and the request back again
						if (allocationMap.isAllocated(slot.getSlotId())) {
							// only re-add if the slot hasn't been removed in the meantime
							freeSlots.put(slot.getSlotId(), removedSlot);
						}
						pendingSlotRequests.put(allocationId, request);
					}
					return null;
				}
			}, resourceManagerServices.getExecutor());
		} else {
			LOG.info("Cannot fulfil slot request, try to allocate a new container for it, " +
				"AllocationID:{}, JobID:{}", allocationId, request.getJobId());
			Preconditions.checkState(resourceManagerServices != null,
				"Attempted to allocate resources but no ResourceManagerServices set.");
			resourceManagerServices.allocateResource(request.getResourceProfile());
			pendingSlotRequests.put(allocationId, request);
		}

		return new SlotRequestRegistered(allocationId);
	}

	/**
	 * Sync slot status with TaskManager's SlotReport.
	 */
	public void updateSlotStatus(final SlotReport slotReport) {
		for (SlotStatus slotStatus : slotReport.getSlotsStatus()) {
			updateSlotStatus(slotStatus);
		}
	}

	/**
	 * Registers a TaskExecutor
	 * @param resourceID TaskExecutor's ResourceID
	 * @param gateway TaskExcutor's gateway
	 */
	public void registerTaskExecutor(ResourceID resourceID, TaskExecutorGateway gateway) {
		this.taskManagerGateways.put(resourceID, gateway);
	}

	/**
	 * The slot request to TaskManager may be either failed by rpc communication (timeout, network error, etc.)
	 * or really rejected by TaskManager. We shall retry this request by:
	 * <ul>
	 * <li>1. verify and clear all the previous allocate information for this request
	 * <li>2. try to request slot again
	 * </ul>
	 * <p>
	 * This may cause some duplicate allocation, e.g. the slot request to TaskManager is successful but the response
	 * is lost somehow, so we may request a slot in another TaskManager, this causes two slots assigned to one request,
	 * but it can be taken care of by rejecting registration at JobManager.
	 *
	 * @param originalRequest The original slot request
	 * @param slotId          The target SlotID
	 */
	public void handleSlotRequestFailedAtTaskManager(final SlotRequest originalRequest, final SlotID slotId) {
		final AllocationID originalAllocationId = originalRequest.getAllocationId();
		LOG.info("Slot request failed at TaskManager, SlotID:{}, AllocationID:{}, JobID:{}",
			slotId, originalAllocationId, originalRequest.getJobId());

		// verify the allocation info before we do anything
		if (freeSlots.containsKey(slotId)) {
			// this slot is currently empty, no need to de-allocate it from our allocations
			LOG.info("Original slot is somehow empty, retrying this request");

			// before retry, we should double check whether this request was allocated by some other ways
			if (!allocationMap.isAllocated(originalAllocationId)) {
				requestSlot(originalRequest);
			} else {
				LOG.info("The failed request has somehow been allocated, SlotID:{}",
					allocationMap.getSlotID(originalAllocationId));
			}
		} else if (allocationMap.isAllocated(slotId)) {
			final AllocationID currentAllocationId = allocationMap.getAllocationID(slotId);

			// check whether we have an agreement on whom this slot belongs to
			if (originalAllocationId.equals(currentAllocationId)) {
				LOG.info("De-allocate this request and retry");
				allocationMap.removeAllocation(currentAllocationId);

				// put this slot back to free pool
				ResourceSlot slot = checkNotNull(getRegisteredSlot(slotId));
				freeSlots.put(slotId, slot);

				// retry the request
				requestSlot(originalRequest);
			} else {
				// the slot is taken by someone else, no need to de-allocate it from our allocations
				LOG.info("Original slot is taken by someone else, current AllocationID:{}", currentAllocationId);

				// before retry, we should double check whether this request was allocated by some other ways
				if (!allocationMap.isAllocated(originalAllocationId)) {
					requestSlot(originalRequest);
				} else {
					LOG.info("The failed request is somehow been allocated, SlotID:{}",
						allocationMap.getSlotID(originalAllocationId));
				}
			}
		} else {
			LOG.error("BUG! {} is neither in free pool nor in allocated pool", slotId);
		}
	}

	/**
	 * Callback for TaskManager failures. In case that a TaskManager fails, we have to clean up all its slots.
	 *
	 * @param resourceId The ResourceID of the TaskManager
	 */
	public void notifyTaskManagerFailure(final ResourceID resourceId) {
		LOG.info("Resource:{} been notified failure", resourceId);
		taskManagerGateways.remove(resourceId);
		final Map<SlotID, ResourceSlot> slotIdsToRemove = registeredSlots.remove(resourceId);
		if (slotIdsToRemove != null) {
			for (SlotID slotId : slotIdsToRemove.keySet()) {
				LOG.info("Removing Slot: {} upon resource failure", slotId);
				if (freeSlots.containsKey(slotId)) {
					freeSlots.remove(slotId);
				} else if (allocationMap.isAllocated(slotId)) {
					allocationMap.removeAllocation(slotId);
				} else {
					LOG.error("BUG! {} is neither in free pool nor in allocated pool", slotId);
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  internal behaviors
	// ------------------------------------------------------------------------

	/**
	 * Update slot status based on TaskManager's report. There are mainly two situations when we receive the report:
	 * <ul>
	 * <li>1. The slot is newly registered.</li>
	 * <li>2. The slot has registered, it contains its current status.</li>
	 * </ul>
	 * <p>
	 * Regarding 1: It's fairly simple, we just record this slot's status, and trigger schedule if slot is empty.
	 * <p>
	 * Regarding 2: It will cause some weird situation since we may have some time-gap on how the slot's status really
	 * is. We may have some updates on the slot's allocation, but it doesn't reflected by TaskManager's heartbeat yet,
	 * and we may make some wrong decision if we cannot guarantee we have the exact status about all the slots. So
	 * the principle here is: We always trust TaskManager's heartbeat, we will correct our information based on that
	 * and take next action based on the diff between our information and heartbeat status.
	 *
	 * @param reportedStatus Reported slot status
	 */
	void updateSlotStatus(final SlotStatus reportedStatus) {
		final SlotID slotId = reportedStatus.getSlotID();

		final TaskExecutorGateway taskExecutorGateway = taskManagerGateways.get(slotId.getResourceID());
		if (taskExecutorGateway == null) {
			LOG.info("Received SlotStatus but ResourceID {} is unknown to the SlotManager",
				slotId.getResourceID());
			return;
		}

		final ResourceSlot slot = new ResourceSlot(slotId, reportedStatus.getProfiler(), taskExecutorGateway);

		if (registerNewSlot(slot)) {
			// we have a newly registered slot
			LOG.info("New slot appeared, SlotID:{}, AllocationID:{}", slotId, reportedStatus.getAllocationID());

			if (reportedStatus.getAllocationID() != null) {
				// slot in use, record this in bookkeeping
				allocationMap.addAllocation(slotId, reportedStatus.getAllocationID());
			} else {
				handleFreeSlot(slot);
			}
		} else {
			// slot exists, update current information
			if (reportedStatus.getAllocationID() != null) {
				// slot is reported in use
				final AllocationID reportedAllocationId = reportedStatus.getAllocationID();

				// check whether we also thought this slot is in use
				if (allocationMap.isAllocated(slotId)) {
					// we also think that slot is in use, check whether the AllocationID matches
					final AllocationID currentAllocationId = allocationMap.getAllocationID(slotId);

					if (!reportedAllocationId.equals(currentAllocationId)) {
						LOG.info("Slot allocation info mismatch! SlotID:{}, current:{}, reported:{}",
							slotId, currentAllocationId, reportedAllocationId);

						// seems we have a disagreement about the slot assignments, need to correct it
						allocationMap.removeAllocation(slotId);
						allocationMap.addAllocation(slotId, reportedAllocationId);
					}
				} else {
					LOG.info("Slot allocation info mismatch! SlotID:{}, current:null, reported:{}",
						slotId, reportedAllocationId);

					// we thought the slot is free, should correct this information
					allocationMap.addAllocation(slotId, reportedStatus.getAllocationID());

					// remove this slot from free slots pool
					freeSlots.remove(slotId);
				}
			} else {
				// slot is reported empty

				// check whether we also thought this slot is empty
				if (allocationMap.isAllocated(slotId)) {
					LOG.info("Slot allocation info mismatch! SlotID:{}, current:{}, reported:null",
						slotId, allocationMap.getAllocationID(slotId));

					// we thought the slot is in use, correct it
					allocationMap.removeAllocation(slotId);

					// we have a free slot!
					handleFreeSlot(slot);
				}
			}
		}
	}

	/**
	 * When we have a free slot, try to fulfill the pending request first. If any request can be fulfilled,
	 * record this allocation in bookkeeping and send slot request to TaskManager, else we just add this slot
	 * to the free pool.
	 *
	 * @param freeSlot The free slot
	 */
	private void handleFreeSlot(final ResourceSlot freeSlot) {
		SlotRequest chosenRequest = chooseRequestToFulfill(freeSlot, pendingSlotRequests);

		if (chosenRequest != null) {
			final AllocationID allocationId = chosenRequest.getAllocationId();
			final SlotRequest removedSlotRequest = pendingSlotRequests.remove(allocationId);

			LOG.info("Assigning SlotID({}) to AllocationID({}), JobID:{}", freeSlot.getSlotId(),
				allocationId, chosenRequest.getJobId());
			allocationMap.addAllocation(freeSlot.getSlotId(), allocationId);

			final Future<SlotRequestReply> slotRequestReplyFuture =
				freeSlot.getTaskExecutorGateway().requestSlot(allocationId, leaderID, timeout);

			slotRequestReplyFuture.handleAsync(new BiFunction<SlotRequestReply, Throwable, Object>() {
				@Override
				public Object apply(SlotRequestReply slotRequestReply, Throwable throwable) {
					if (throwable != null) {
						// we failed, add the request back again
						if (allocationMap.isAllocated(freeSlot.getSlotId())) {
							pendingSlotRequests.put(allocationId, removedSlotRequest);
						}
					}
					return null;
				}
			}, resourceManagerServices.getExecutor());
		} else {
			freeSlots.put(freeSlot.getSlotId(), freeSlot);
		}
	}

	/**
	 * Check whether the request is duplicated. We use AllocationID to identify slot request, for each
	 * formerly received slot request, it is either in pending list or already been allocated.
	 *
	 * @param request The slot request
	 * @return <tt>true</tt> if the request is duplicated
	 */
	private boolean isRequestDuplicated(final SlotRequest request) {
		final AllocationID allocationId = request.getAllocationId();
		return pendingSlotRequests.containsKey(allocationId)
			|| allocationMap.isAllocated(allocationId);
	}

	/**
	 * Try to register slot, and tell if this slot is newly registered.
	 *
	 * @param slot The ResourceSlot which will be checked and registered
	 * @return <tt>true</tt> if we meet a new slot
	 */
	private boolean registerNewSlot(final ResourceSlot slot) {
		final SlotID slotId = slot.getSlotId();
		final ResourceID resourceId = slotId.getResourceID();
		if (!registeredSlots.containsKey(resourceId)) {
			registeredSlots.put(resourceId, new HashMap<SlotID, ResourceSlot>());
		}
		return registeredSlots.get(resourceId).put(slotId, slot) == null;
	}

	private ResourceSlot getRegisteredSlot(final SlotID slotId) {
		final ResourceID resourceId = slotId.getResourceID();
		if (!registeredSlots.containsKey(resourceId)) {
			return null;
		}
		return registeredSlots.get(resourceId).get(slotId);
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Choose a slot to use among all free slots, the behavior is framework specified.
	 *
	 * @param request   The slot request
	 * @param freeSlots All slots which can be used
	 * @return The slot we choose to use, <tt>null</tt> if we did not find a match
	 */
	protected abstract ResourceSlot chooseSlotToUse(final SlotRequest request,
		final Map<SlotID, ResourceSlot> freeSlots);

	/**
	 * Choose a pending request to fulfill when we have a free slot, the behavior is framework specified.
	 *
	 * @param offeredSlot     The free slot
	 * @param pendingRequests All the pending slot requests
	 * @return The chosen SlotRequest, <tt>null</tt> if we did not find a match
	 */
	protected abstract SlotRequest chooseRequestToFulfill(final ResourceSlot offeredSlot,
		final Map<AllocationID, SlotRequest> pendingRequests);

	// ------------------------------------------------------------------------
	//  Helper classes
	// ------------------------------------------------------------------------

	/**
	 * We maintain all the allocations with SlotID and AllocationID. We are able to get or remove the allocation info
	 * either by SlotID or AllocationID.
	 */
	private static class AllocationMap {

		/** All allocated slots (by SlotID) */
		private final Map<SlotID, AllocationID> allocatedSlots;

		/** All allocated slots (by AllocationID), it'a a inverse view of allocatedSlots */
		private final Map<AllocationID, SlotID> allocatedSlotsByAllocationId;

		AllocationMap() {
			this.allocatedSlots = new HashMap<>(16);
			this.allocatedSlotsByAllocationId = new HashMap<>(16);
		}

		/**
		 * Add a allocation
		 *
		 * @param slotId       The slot id
		 * @param allocationId The allocation id
		 */
		void addAllocation(final SlotID slotId, final AllocationID allocationId) {
			allocatedSlots.put(slotId, allocationId);
			allocatedSlotsByAllocationId.put(allocationId, slotId);
		}

		/**
		 * De-allocation with slot id
		 *
		 * @param slotId The slot id
		 */
		void removeAllocation(final SlotID slotId) {
			if (allocatedSlots.containsKey(slotId)) {
				final AllocationID allocationId = allocatedSlots.get(slotId);
				allocatedSlots.remove(slotId);
				allocatedSlotsByAllocationId.remove(allocationId);
			}
		}

		/**
		 * De-allocation with allocation id
		 *
		 * @param allocationId The allocation id
		 */
		void removeAllocation(final AllocationID allocationId) {
			if (allocatedSlotsByAllocationId.containsKey(allocationId)) {
				SlotID slotId = allocatedSlotsByAllocationId.get(allocationId);
				allocatedSlotsByAllocationId.remove(allocationId);
				allocatedSlots.remove(slotId);
			}
		}

		/**
		 * Check whether allocation exists by slot id
		 *
		 * @param slotId The slot id
		 * @return true if the allocation exists
		 */
		boolean isAllocated(final SlotID slotId) {
			return allocatedSlots.containsKey(slotId);
		}

		/**
		 * Check whether allocation exists by allocation id
		 *
		 * @param allocationId The allocation id
		 * @return true if the allocation exists
		 */
		boolean isAllocated(final AllocationID allocationId) {
			return allocatedSlotsByAllocationId.containsKey(allocationId);
		}

		AllocationID getAllocationID(final SlotID slotId) {
			return allocatedSlots.get(slotId);
		}

		SlotID getSlotID(final AllocationID allocationId) {
			return allocatedSlotsByAllocationId.get(allocationId);
		}

		public int size() {
			return allocatedSlots.size();
		}

		public void clear() {
			allocatedSlots.clear();
			allocatedSlotsByAllocationId.clear();
		}
	}

	/**
	 * Clears the state of the SlotManager after leadership revokal
	 */
	public void clearState() {
		taskManagerGateways.clear();
		registeredSlots.clear();
		pendingSlotRequests.clear();
		freeSlots.clear();
		allocationMap.clear();
		leaderID = null;
	}

	// ------------------------------------------------------------------------
	//  High availability (called by the ResourceManager)
	// ------------------------------------------------------------------------

	public void setLeaderUUID(UUID leaderSessionID) {
		this.leaderID = leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Testing utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	boolean isAllocated(final SlotID slotId) {
		return allocationMap.isAllocated(slotId);
	}

	@VisibleForTesting
	boolean isAllocated(final AllocationID allocationId) {
		return allocationMap.isAllocated(allocationId);
	}

	/**
	 * Add free slots directly to the free pool, this will not trigger pending requests allocation
	 *
	 * @param slot The resource slot
	 */
	@VisibleForTesting
	void addFreeSlot(final ResourceSlot slot) {
		final ResourceID resourceId = slot.getResourceID();
		final SlotID slotId = slot.getSlotId();

		if (!registeredSlots.containsKey(resourceId)) {
			registeredSlots.put(resourceId, new HashMap<SlotID, ResourceSlot>());
		}
		registeredSlots.get(resourceId).put(slot.getSlotId(), slot);
		freeSlots.put(slotId, slot);
	}

	@VisibleForTesting
	int getAllocatedSlotCount() {
		return allocationMap.size();
	}

	@VisibleForTesting
	int getFreeSlotCount() {
		return freeSlots.size();
	}

	@VisibleForTesting
	int getPendingRequestCount() {
		return pendingSlotRequests.size();
	}
}
