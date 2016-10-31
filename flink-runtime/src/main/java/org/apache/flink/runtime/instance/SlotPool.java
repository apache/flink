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

package org.apache.flink.runtime.instance;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ApplyFunction;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestRejected;
import org.apache.flink.runtime.resourcemanager.messages.jobmanager.RMSlotRequestReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The slot pool serves slot request issued by Scheduler or ExecutionGraph. It will will attempt to acquire new slots
 * from the ResourceManager when it cannot serve a slot request. If no ResourceManager is currently available,
 * or it gets a decline from the ResourceManager, or a request times out, it fails the slot request. The slot pool also
 * holds all the slots that were offered to it and accepted, and can thus provides registered free slots even if the
 * ResourceManager is down. The slots will only be released when they are useless, e.g. when the job is fully running
 * but we still have some free slots.
 * <p>
 * All the allocation or the slot offering will be identified by self generated AllocationID, we will use it to
 * eliminate ambiguities.
 */
public class SlotPool implements SlotOwner {

	private static final Logger LOG = LoggerFactory.getLogger(SlotPool.class);

	private final Object lock = new Object();

	/** The executor which is used to execute futures */
	private final Executor executor;

	/** All registered resources, slots will be accepted and used only if the resource is registered */
	private final Set<ResourceID> registeredResources;

	/** The book-keeping of all allocated slots */
	private final AllocatedSlots allocatedSlots;

	/** The book-keeping of all available slots */
	private final AvailableSlots availableSlots;

	/** All pending requests waiting for slots */
	private final Map<AllocationID, Tuple2<SlotRequest, FlinkCompletableFuture<SlotDescriptor>>> pendingRequests;

	/** Timeout of slot allocation */
	private final Time timeout;

	/** the leader id of job manager */
	private UUID jobManagerLeaderId;

	/** The leader id of resource manager */
	private UUID resourceManagerLeaderId;

	/** The gateway to communicate with resource manager */
	private ResourceManagerGateway resourceManagerGateway;

	public SlotPool(final Executor executor) {
		this.executor = executor;
		this.registeredResources = new HashSet<>();
		this.allocatedSlots = new AllocatedSlots();
		this.availableSlots = new AvailableSlots();
		this.pendingRequests = new HashMap<>();
		this.timeout = Time.of(5, TimeUnit.SECONDS);
	}

	public void setJobManagerLeaderId(final UUID jobManagerLeaderId) {
		this.jobManagerLeaderId = jobManagerLeaderId;
	}

	// ------------------------------------------------------------------------
	//  Slot Allocation
	// ------------------------------------------------------------------------

	/**
	 * Try to allocate a simple slot with specified resource profile.
	 *
	 * @param jobID           The job id which the slot allocated for
	 * @param resourceProfile The needed resource profile
	 * @return The future of allocated simple slot
	 */
	public Future<SimpleSlot> allocateSimpleSlot(final JobID jobID, final ResourceProfile resourceProfile) {
		return allocateSimpleSlot(jobID, resourceProfile, new AllocationID());
	}


	/**
	 * Try to allocate a simple slot with specified resource profile and specified allocation id. It's mainly
	 * for testing purpose since we need to specify whatever allocation id we want.
	 */
	@VisibleForTesting
	Future<SimpleSlot> allocateSimpleSlot(
		final JobID jobID,
		final ResourceProfile resourceProfile,
		final AllocationID allocationID)
	{
		final FlinkCompletableFuture<SlotDescriptor> future = new FlinkCompletableFuture<>();

		internalAllocateSlot(jobID, allocationID, resourceProfile, future);

		return future.thenApplyAsync(
			new ApplyFunction<SlotDescriptor, SimpleSlot>() {
				@Override
				public SimpleSlot apply(SlotDescriptor descriptor) {
					SimpleSlot slot = new SimpleSlot(
							descriptor.getJobID(), SlotPool.this,
							descriptor.getTaskManagerLocation(), descriptor.getSlotNumber(),
							descriptor.getTaskManagerGateway());
					synchronized (lock) {
						// double validation since we are out of the lock protection after the slot is granted
						if (registeredResources.contains(descriptor.getTaskManagerLocation().getResourceID())) {
							LOG.info("Allocation[{}] Allocated simple slot: {} for job {}.", allocationID, slot, jobID);
							allocatedSlots.add(allocationID, descriptor, slot);
						}
						else {
							throw new RuntimeException("Resource was marked dead asynchronously.");
						}
					}
					return slot;
				}
			},
			executor
		);
	}


	/**
	 * Try to allocate a shared slot with specified resource profile.
	 *
	 * @param jobID                  The job id which the slot allocated for
	 * @param resourceProfile        The needed resource profile
	 * @param sharingGroupAssignment The slot sharing group of the vertex
	 * @return The future of allocated shared slot
	 */
	public Future<SharedSlot> allocateSharedSlot(
		final JobID jobID,
		final ResourceProfile resourceProfile,
		final SlotSharingGroupAssignment sharingGroupAssignment)
	{
		return allocateSharedSlot(jobID, resourceProfile, sharingGroupAssignment, new AllocationID());
	}

	/**
	 * Try to allocate a shared slot with specified resource profile and specified allocation id. It's mainly
	 * for testing purpose since we need to specify whatever allocation id we want.
	 */
	@VisibleForTesting
	Future<SharedSlot> allocateSharedSlot(
		final JobID jobID,
		final ResourceProfile resourceProfile,
		final SlotSharingGroupAssignment sharingGroupAssignment,
		final AllocationID allocationID)
	{
		final FlinkCompletableFuture<SlotDescriptor> future = new FlinkCompletableFuture<>();

		internalAllocateSlot(jobID, allocationID, resourceProfile, future);

		return future.thenApplyAsync(
			new ApplyFunction<SlotDescriptor, SharedSlot>() {
				@Override
				public SharedSlot apply(SlotDescriptor descriptor) {
					SharedSlot slot = new SharedSlot(
							descriptor.getJobID(), SlotPool.this, descriptor.getTaskManagerLocation(),
							descriptor.getSlotNumber(), descriptor.getTaskManagerGateway(),
							sharingGroupAssignment);

					synchronized (lock) {
						// double validation since we are out of the lock protection after the slot is granted
						if (registeredResources.contains(descriptor.getTaskManagerLocation().getResourceID())) {
							LOG.info("Allocation[{}] Allocated shared slot: {} for job {}.", allocationID, slot, jobID);
							allocatedSlots.add(allocationID, descriptor, slot);
						}
						else {
							throw new RuntimeException("Resource was marked dead asynchronously.");
						}
					}
					return slot;
				}
			},
			executor
		);
	}

	/**
	 * Internally allocate the slot with specified resource profile. We will first check whether we have some
	 * free slot which can meet the requirement already and allocate it immediately. Otherwise, we will try to
	 * allocation the slot from resource manager.
	 */
	private void internalAllocateSlot(
		final JobID jobID,
		final AllocationID allocationID,
		final ResourceProfile resourceProfile,
		final FlinkCompletableFuture<SlotDescriptor> future)
	{
		LOG.info("Allocation[{}] Allocating slot with {} for Job {}.", allocationID, resourceProfile, jobID);

		synchronized (lock) {
			// check whether we have any free slot which can match the required resource profile
			SlotDescriptor freeSlot = availableSlots.poll(resourceProfile);
			if (freeSlot != null) {
				future.complete(freeSlot);
			}
			else {
				if (resourceManagerGateway != null) {
					LOG.info("Allocation[{}] No available slot exists, trying to allocate from resource manager.",
						allocationID);
					SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile);
					pendingRequests.put(allocationID, new Tuple2<>(slotRequest, future));
					resourceManagerGateway.requestSlot(jobManagerLeaderId, resourceManagerLeaderId, slotRequest, timeout)
						.handleAsync(new BiFunction<RMSlotRequestReply, Throwable, Void>() {
							@Override
							public Void apply(RMSlotRequestReply slotRequestReply, Throwable throwable) {
								if (throwable != null) {
									future.completeExceptionally(
										new Exception("Slot allocation from resource manager failed", throwable));
								} else if (slotRequestReply instanceof RMSlotRequestRejected) {
									future.completeExceptionally(
										new Exception("Slot allocation rejected by resource manager"));
								}
								return null;
							}
						}, executor);
				}
				else {
					LOG.warn("Allocation[{}] Resource manager not available right now.", allocationID);
					future.completeExceptionally(new Exception("Resource manager not available right now."));
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Slot releasing & offering
	// ------------------------------------------------------------------------

	/**
	 * Return the slot back to this pool without releasing it. It's mainly called by failed / cancelled tasks, and the
	 * slot can be reused by other pending requests if the resource profile matches.n
	 *
	 * @param slot The slot needs to be returned
	 * @return True if the returning slot been accepted
	 */
	@Override
	public boolean returnAllocatedSlot(Slot slot) {
		checkNotNull(slot);
		checkArgument(!slot.isAlive(), "slot is still alive");
		checkArgument(slot.getOwner() == this, "slot belongs to the wrong pool.");

		if (slot.markReleased()) {
			synchronized (lock) {
				final SlotDescriptor slotDescriptor = allocatedSlots.remove(slot);
				if (slotDescriptor != null) {
					// check if this TaskManager is valid
					if (!registeredResources.contains(slot.getTaskManagerID())) {
						return false;
					}

					final FlinkCompletableFuture<SlotDescriptor> pendingRequest = pollPendingRequest(slotDescriptor);
					if (pendingRequest != null) {
						pendingRequest.complete(slotDescriptor);
					}
					else {
						availableSlots.add(slotDescriptor);
					}

					return true;
				}
				else {
					throw new IllegalArgumentException("Slot was not allocated from this pool.");
				}
			}
		}
		else {
			return false;
		}
	}

	private FlinkCompletableFuture<SlotDescriptor> pollPendingRequest(final SlotDescriptor slotDescriptor) {
		for (Map.Entry<AllocationID, Tuple2<SlotRequest, FlinkCompletableFuture<SlotDescriptor>>> entry : pendingRequests.entrySet()) {
			final Tuple2<SlotRequest, FlinkCompletableFuture<SlotDescriptor>> pendingRequest = entry.getValue();
			if (slotDescriptor.getResourceProfile().isMatching(pendingRequest.f0.getResourceProfile())) {
				pendingRequests.remove(entry.getKey());
				return pendingRequest.f1;
			}
		}
		return null;
	}

	/**
	 * Release slot to TaskManager, called for finished tasks or canceled jobs.
	 *
	 * @param slot The slot needs to be released.
	 */
	public void releaseSlot(final Slot slot) {
		synchronized (lock) {
			allocatedSlots.remove(slot);
			availableSlots.remove(new SlotDescriptor(slot));
			// TODO: send release request to task manager
		}
	}

	/**
	 * Slot offering by TaskManager with AllocationID. The AllocationID is originally generated by this pool and
	 * transfer through the ResourceManager to TaskManager. We use it to distinguish the different allocation
	 * we issued. Slot offering may be rejected if we find something mismatching or there is actually no pending
	 * request waiting for this slot (maybe fulfilled by some other returned slot).
	 *
	 * @param allocationID   The allocation id of the lo
	 * @param slotDescriptor The offered slot descriptor
	 * @return True if we accept the offering
	 */
	public boolean offerSlot(final AllocationID allocationID, final SlotDescriptor slotDescriptor) {
		synchronized (lock) {
			// check if this TaskManager is valid
			final ResourceID resourceID = slotDescriptor.getTaskManagerLocation().getResourceID();
			if (!registeredResources.contains(resourceID)) {
				LOG.warn("Allocation[{}] Slot offering from unregistered TaskManager: {}",
					allocationID, slotDescriptor);
				return false;
			}

			// check whether we have already using this slot
			final Slot allocatedSlot = allocatedSlots.get(allocationID);
			if (allocatedSlot != null) {
				final SlotDescriptor allocatedSlotDescriptor = new SlotDescriptor(allocatedSlot);

				if (allocatedSlotDescriptor.equals(slotDescriptor)) {
					LOG.debug("Allocation[{}] Duplicated slot offering: {}",
						allocationID, slotDescriptor);
					return true;
				}
				else {
					LOG.info("Allocation[{}] Allocation had been fulfilled by slot {}, rejecting offered slot {}",
						allocationID, allocatedSlotDescriptor, slotDescriptor);
					return false;
				}
			}

			// check whether we already have this slot in free pool
			if (availableSlots.contains(slotDescriptor)) {
				LOG.debug("Allocation[{}] Duplicated slot offering: {}",
					allocationID, slotDescriptor);
				return true;
			}

			// check whether we have request waiting for this slot
			if (pendingRequests.containsKey(allocationID)) {
				FlinkCompletableFuture<SlotDescriptor> future = pendingRequests.remove(allocationID).f1;
				future.complete(slotDescriptor);
				return true;
			}

			// unwanted slot, rejecting this offer
			return false;
		}
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Fail the specified allocation and release the corresponding slot if we have one.
	 * This may triggered by JobManager when some slot allocation failed with timeout.
	 * Or this could be triggered by TaskManager, when it finds out something went wrong with the slot,
	 * and decided to take it back.
	 *
	 * @param allocationID Represents the allocation which should be failed
	 * @param cause        The cause of the failure
	 */
	public void failAllocation(final AllocationID allocationID, final Exception cause) {
		synchronized (lock) {
			// 1. check whether the allocation still pending
			Tuple2<SlotRequest, FlinkCompletableFuture<SlotDescriptor>> pendingRequest =
					pendingRequests.get(allocationID);
			if (pendingRequest != null) {
				pendingRequest.f1.completeExceptionally(cause);
				return;
			}

			// 2. check whether we have a free slot corresponding to this allocation id
			// TODO: add allocation id to slot descriptor, so we can remove it by allocation id

			// 3. check whether we have a in-use slot corresponding to this allocation id
			// TODO: needs mechanism to release the in-use Slot but don't return it back to this pool

			// TODO: add some unit tests when the previous two are ready, the allocation may failed at any phase
		}
	}

	// ------------------------------------------------------------------------
	//  Resource
	// ------------------------------------------------------------------------

	/**
	 * Register TaskManager to this pool, only those slots come from registered TaskManager will be considered valid.
	 * Also it provides a way for us to keep "dead" or "abnormal" TaskManagers out of this pool.
	 *
	 * @param resourceID The id of the TaskManager
	 */
	public void registerResource(final ResourceID resourceID) {
		synchronized (lock) {
			registeredResources.add(resourceID);
		}
	}

	/**
	 * Unregister TaskManager from this pool, all the related slots will be released and tasks be canceled. Called
	 * when we find some TaskManager becomes "dead" or "abnormal", and we decide to not using slots from it anymore.
	 *
	 * @param resourceID The id of the TaskManager
	 */
	public void releaseResource(final ResourceID resourceID) {
		synchronized (lock) {
			registeredResources.remove(resourceID);
			availableSlots.removeByResource(resourceID);

			final Set<Slot> allocatedSlotsForResource = allocatedSlots.getSlotsByResource(resourceID);
			for (Slot slot : allocatedSlotsForResource) {
				slot.releaseSlot();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  ResourceManager
	// ------------------------------------------------------------------------

	public void setResourceManager(
		final UUID resourceManagerLeaderId,
		final ResourceManagerGateway resourceManagerGateway)
	{
		synchronized (lock) {
			this.resourceManagerLeaderId = resourceManagerLeaderId;
			this.resourceManagerGateway = resourceManagerGateway;
		}
	}

	public void disconnectResourceManager() {
		synchronized (lock) {
			this.resourceManagerLeaderId = null;
			this.resourceManagerGateway = null;
		}
	}

	// ------------------------------------------------------------------------
	//  Helper classes
	// ------------------------------------------------------------------------

	/**
	 * Organize allocated slots from different points of view.
	 */
	static class AllocatedSlots {

		/** All allocated slots organized by TaskManager's id */
		private final Map<ResourceID, Set<Slot>> allocatedSlotsByResource;

		/** All allocated slots organized by Slot object */
		private final Map<Slot, AllocationID> allocatedSlots;

		/** All allocated slot descriptors organized by Slot object */
		private final Map<Slot, SlotDescriptor> allocatedSlotsWithDescriptor;

		/** All allocated slots organized by AllocationID */
		private final Map<AllocationID, Slot> allocatedSlotsById;

		AllocatedSlots() {
			this.allocatedSlotsByResource = new HashMap<>();
			this.allocatedSlots = new HashMap<>();
			this.allocatedSlotsWithDescriptor = new HashMap<>();
			this.allocatedSlotsById = new HashMap<>();
		}

		/**
		 * Add a new allocation
		 *
		 * @param allocationID The allocation id
		 * @param slot         The allocated slot
		 */
		void add(final AllocationID allocationID, final SlotDescriptor descriptor, final Slot slot) {
			allocatedSlots.put(slot, allocationID);
			allocatedSlotsById.put(allocationID, slot);
			allocatedSlotsWithDescriptor.put(slot, descriptor);

			final ResourceID resourceID = slot.getTaskManagerID();
			Set<Slot> slotsForResource = allocatedSlotsByResource.get(resourceID);
			if (slotsForResource == null) {
				slotsForResource = new HashSet<>();
				allocatedSlotsByResource.put(resourceID, slotsForResource);
			}
			slotsForResource.add(slot);
		}

		/**
		 * Get allocated slot with allocation id
		 *
		 * @param allocationID The allocation id
		 * @return The allocated slot, null if we can't find a match
		 */
		Slot get(final AllocationID allocationID) {
			return allocatedSlotsById.get(allocationID);
		}

		/**
		 * Check whether we have allocated this slot
		 *
		 * @param slot The slot needs to checked
		 * @return True if we contains this slot
		 */
		boolean contains(final Slot slot) {
			return allocatedSlots.containsKey(slot);
		}

		/**
		 * Remove an allocation with slot.
		 *
		 * @param slot The slot needs to be removed
		 */
		SlotDescriptor remove(final Slot slot) {
			final SlotDescriptor descriptor = allocatedSlotsWithDescriptor.remove(slot);
			if (descriptor != null) {
				final AllocationID allocationID = allocatedSlots.remove(slot);
				if (allocationID != null) {
					allocatedSlotsById.remove(allocationID);
				} else {
					throw new IllegalStateException("Bug: maps are inconsistent");
				}

				final ResourceID resourceID = slot.getTaskManagerID();
				final Set<Slot> slotsForResource = allocatedSlotsByResource.get(resourceID);
				slotsForResource.remove(slot);
				if (slotsForResource.isEmpty()) {
					allocatedSlotsByResource.remove(resourceID);
				}
				
				return descriptor;
			} else {
				return null;
			}
		}

		/**
		 * Get all allocated slot from same TaskManager.
		 *
		 * @param resourceID The id of the TaskManager
		 * @return Set of slots which are allocated from the same TaskManager
		 */
		Set<Slot> getSlotsByResource(final ResourceID resourceID) {
			Set<Slot> slotsForResource = allocatedSlotsByResource.get(resourceID);
			if (slotsForResource != null) {
				return new HashSet<>(slotsForResource);
			}
			else {
				return new HashSet<>();
			}
		}

		@VisibleForTesting
		boolean containResource(final ResourceID resourceID) {
			return allocatedSlotsByResource.containsKey(resourceID);
		}

		@VisibleForTesting
		int size() {
			return allocatedSlots.size();
		}
	}

	/**
	 * Organize all available slots from different points of view.
	 */
	static class AvailableSlots {

		/** All available slots organized by TaskManager */
		private final Map<ResourceID, Set<SlotDescriptor>> availableSlotsByResource;

		/** All available slots */
		private final Set<SlotDescriptor> availableSlots;

		AvailableSlots() {
			this.availableSlotsByResource = new HashMap<>();
			this.availableSlots = new HashSet<>();
		}

		/**
		 * Add an available slot.
		 *
		 * @param descriptor The descriptor of the slot
		 */
		void add(final SlotDescriptor descriptor) {
			availableSlots.add(descriptor);

			final ResourceID resourceID = descriptor.getTaskManagerLocation().getResourceID();
			Set<SlotDescriptor> slotsForResource = availableSlotsByResource.get(resourceID);
			if (slotsForResource == null) {
				slotsForResource = new HashSet<>();
				availableSlotsByResource.put(resourceID, slotsForResource);
			}
			slotsForResource.add(descriptor);
		}

		/**
		 * Check whether we have this slot
		 *
		 * @param slotDescriptor The descriptor of the slot
		 * @return True if we contains this slot
		 */
		boolean contains(final SlotDescriptor slotDescriptor) {
			return availableSlots.contains(slotDescriptor);
		}

		/**
		 * Poll a slot which matches the required resource profile
		 *
		 * @param resourceProfile The required resource profile
		 * @return Slot which matches the resource profile, null if we can't find a match
		 */
		SlotDescriptor poll(final ResourceProfile resourceProfile) {
			for (SlotDescriptor slotDescriptor : availableSlots) {
				if (slotDescriptor.getResourceProfile().isMatching(resourceProfile)) {
					remove(slotDescriptor);
					return slotDescriptor;
				}
			}
			return null;
		}

		/**
		 * Remove all available slots come from specified TaskManager.
		 *
		 * @param resourceID The id of the TaskManager
		 */
		void removeByResource(final ResourceID resourceID) {
			final Set<SlotDescriptor> slotsForResource = availableSlotsByResource.remove(resourceID);
			if (slotsForResource != null) {
				for (SlotDescriptor slotDescriptor : slotsForResource) {
					availableSlots.remove(slotDescriptor);
				}
			}
		}

		private void remove(final SlotDescriptor slotDescriptor) {
			availableSlots.remove(slotDescriptor);

			final ResourceID resourceID = slotDescriptor.getTaskManagerLocation().getResourceID();
			final Set<SlotDescriptor> slotsForResource = checkNotNull(availableSlotsByResource.get(resourceID));
			slotsForResource.remove(slotDescriptor);
			if (slotsForResource.isEmpty()) {
				availableSlotsByResource.remove(resourceID);
			}
		}

		@VisibleForTesting
		boolean containResource(final ResourceID resourceID) {
			return availableSlotsByResource.containsKey(resourceID);
		}

		@VisibleForTesting
		int size() {
			return availableSlots.size();
		}
	}
}
