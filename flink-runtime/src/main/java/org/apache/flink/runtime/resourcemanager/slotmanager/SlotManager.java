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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.CompletableFuture;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or and
 * allocated slot is freed, then it tries to fulfill another pending slot request. Whenever there
 * are not enough slots available the slot manager will notify the resource manager about it via
 * {@link ResourceManagerActions#allocateResource(ResourceProfile)}.
 *
 * In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and not fulfilled pending slot requests time out triggering their
 * release and failure, respectively.
 */
public class SlotManager implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(SlotManager.class);

	/** Scheduled executor for timeouts */
	private final ScheduledExecutor scheduledExecutor;

	/** Timeout for slot requests to the task manager */
	private final Time taskManagerRequestTimeout;

	/** Timeout after which an allocation is discarded */
	private final Time slotRequestTimeout;

	/** Timeout after which an unused TaskManager is released */
	private final Time taskManagerTimeout;

	/** Map for all registered slots */
	private final HashMap<SlotID, TaskManagerSlot> slots;

	/** Index of all currently free slots */
	private final LinkedHashMap<SlotID, TaskManagerSlot> freeSlots;

	/** All currently registered task managers */
	private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

	/** Map of fulfilled and active allocations for request deduplication purposes */
	private final HashMap<AllocationID, SlotID> fulfilledSlotRequests;

	/** Map of pending/unfulfilled slot allocation requests */
	private final HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests;

	/** Leader id of the containing component */
	private UUID leaderId;

	/** Executor for future callbacks which have to be "synchronized" */
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations */
	private ResourceManagerActions resourceManagerActions;

	/** True iff the component has been started */
	private boolean started;

	public SlotManager(
			ScheduledExecutor scheduledExecutor,
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout) {
		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);

		slots = new HashMap<>(16);
		freeSlots = new LinkedHashMap<>(16);
		taskManagerRegistrations = new HashMap<>(4);
		fulfilledSlotRequests = new HashMap<>(16);
		pendingSlotRequests = new HashMap<>(16);

		leaderId = null;
		resourceManagerActions = null;
		started = false;
	}

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newLeaderId to use for communication with the task managers
	 * @param newResourceManagerActions to use for resource (de-)allocations
	 */
	public void start(UUID newLeaderId, Executor newMainThreadExecutor, ResourceManagerActions newResourceManagerActions) {
		leaderId = Preconditions.checkNotNull(newLeaderId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceManagerActions = Preconditions.checkNotNull(newResourceManagerActions);

		started = true;
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	public void suspend() {
		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			cancelPendingSlotRequest(pendingSlotRequest);
		}

		pendingSlotRequests.clear();

		ArrayList<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

		for (InstanceID registeredTaskManager : registeredTaskManagers) {
			unregisterTaskManager(registeredTaskManager);
		}

		leaderId = null;
		resourceManagerActions = null;
		started = false;
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		suspend();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	/**
	 * Requests a slot with the respective resource profile.
	 *
	 * @param slotRequest specifying the requested slot specs
	 * @return true if the slot request was registered; false if the request is a duplicate
	 * @throws SlotManagerException if the slot request failed (e.g. not enough resources left)
	 */
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		checkInit();

		if (checkDuplicateRequest(slotRequest.getAllocationId())) {
			LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

			return false;
		} else {
			PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);

			pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);

			try {
				internalRequestSlot(pendingSlotRequest);
			} catch (ResourceManagerException e) {
				// requesting the slot failed --> remove pending slot request
				pendingSlotRequests.remove(slotRequest.getAllocationId());

				throw new SlotManagerException("Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
			}

			return true;
		}
	}

	/**
	 * Cancels and removes a pending slot request with the given allocation id. If there is no such
	 * pending request, then nothing is done.
	 *
	 * @param allocationId identifying the pending slot request
	 * @return True if a pending slot request was found; otherwise false
	 */
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		checkInit();

		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

		if (null != pendingSlotRequest) {
			cancelPendingSlotRequest(pendingSlotRequest);

			return true;
		} else {
			LOG.debug("No pending slot request with allocation id {} found.", allocationId);

			return false;
		}
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 */
	public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		// we identify task managers by their instance id
		if (!taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(taskExecutorConnection);
			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);
		}

		reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
	}

	/**
	 * Unregisters the task manager identified by the given instance id and its associated slots
	 * from the slot manager.
	 *
	 * @param instanceId identifying the task manager to unregister
	 * @return True if there existed a registered task manager with the given instance id
	 */
	public boolean unregisterTaskManager(InstanceID instanceId) {
		checkInit();

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

		if (null != taskManagerRegistration) {
			removeSlots(taskManagerRegistration.getSlots());

			taskManagerRegistration.cancelTimeout();

			return true;
		} else {
			LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

			return false;
		}
	}

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 */
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		checkInit();

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {
			ArrayList<SlotID> slotsToRemove = new ArrayList<>(taskManagerRegistration.getSlots());
			boolean idle = true;

			for (SlotStatus slotStatus : slotReport) {
				if (slotsToRemove.remove(slotStatus.getSlotID())) {
					// slot which was already registered
					updateSlot(slotStatus.getSlotID(), slotStatus.getAllocationID());
				} else {
					// new slot
					registerSlot(
						taskManagerRegistration,
						slotStatus.getSlotID(),
						slotStatus.getAllocationID(),
						slotStatus.getResourceProfile(),
						taskManagerRegistration.getTaskManagerConnection());
				}

				TaskManagerSlot slot = slots.get(slotStatus.getSlotID());

				idle &= slot.isFree();
			}

			// remove the slots for which we haven't received a slot status message
			removeSlots(slotsToRemove);

			if (idle) {
				// no slot of this task manager is being used --> register timer to free this resource
				registerTaskManagerTimeout(taskManagerRegistration);
			}

			return true;
		} else {
			LOG.debug("Received slot report for unknown task manager with instance id {}. Ignoring this report.", instanceId);

			return false;
		}
	}

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		checkInit();

		TaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			if (slot.isAllocated()) {
				if (Objects.equals(allocationId, slot.getAllocationId())) {
					// free the slot
					slot.setAllocationId(null);
					fulfilledSlotRequests.remove(allocationId);

					if (slot.isFree()) {
						handleFreeSlot(slot);
					}

					TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

					if (null != taskManagerRegistration && !anySlotUsed(taskManagerRegistration.getSlots())) {
						registerTaskManagerTimeout(taskManagerRegistration);
					}
				} else {
					LOG.debug("Received request to free slot {} with expected allocation id {}, " +
						"but actual allocation id {} differs. Ignoring the request.", slotId, allocationId, slot.getAllocationId());
				}
			} else {
				LOG.debug("Slot {} has not been allocated.", allocationId);
			}
		} else {
			LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Behaviour methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Finds a matching slot request for a given resource profile. If there is no such request,
	 * the method returns null.
	 *
	 * Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param slotResourceProfile defining the resources of an available slot
	 * @return A matching slot request which can be deployed in a slot with the given resource
	 * profile. Null if there is no such slot request pending.
	 */
	protected PendingSlotRequest findMatchingRequest(ResourceProfile slotResourceProfile) {

		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			if (!pendingSlotRequest.isAssigned() && slotResourceProfile.isMatching(pendingSlotRequest.getResourceProfile())) {
				return pendingSlotRequest;
			}
		}

		return null;
	}

	/**
	 * Finds a matching slot for a given resource profile. A matching slot has at least as many
	 * resources available as the given resource profile. If there is no such slot available, then
	 * the method returns null.
	 *
	 * Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param requestResourceProfile specifying the resource requirements for the a slot request
	 * @return A matching slot which fulfills the given resource profile. Null if there is no such
	 * slot available.
	 */
	protected TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = freeSlots.entrySet().iterator();

		while (iterator.hasNext()) {
			TaskManagerSlot taskManagerSlot = iterator.next().getValue();

			// sanity check
			Preconditions.checkState(taskManagerSlot.isFree());

			if (taskManagerSlot.getResourceProfile().isMatching(requestResourceProfile)) {
				iterator.remove();
				return taskManagerSlot;
			}
		}

		return null;
	}

	// ---------------------------------------------------------------------------------------------
	// Internal slot operations
	// ---------------------------------------------------------------------------------------------

	/**
	 * Registers a slot for the given task manager at the slot manager. The slot is identified by
	 * the given slot id. The given resource profile defines the available resources for the slot.
	 * The task manager connection can be used to communicate with the task manager.
	 *
	 * @param taskManagerRegistration Task manager for which to register the given slot
	 * @param slotId identifying the slot on the task manager
	 * @param allocationId which is currently deployed in the slot
	 * @param resourceProfile of the slot
	 * @param taskManagerConnection to communicate with the remote task manager
	 */
	private void registerSlot(
			TaskManagerRegistration taskManagerRegistration,
			SlotID slotId,
			AllocationID allocationId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection) {

		Preconditions.checkNotNull(taskManagerRegistration);

		TaskManagerSlot slot = new TaskManagerSlot(
			slotId,
			resourceProfile,
			taskManagerConnection,
			allocationId);

		slots.put(slotId, slot);

		taskManagerRegistration.addSlot(slotId);

		if (slot.isFree()) {
			handleFreeSlot(slot);
		}

		if (slot.isAllocated()) {
			fulfilledSlotRequests.put(slot.getAllocationId(), slotId);
		}
	}

	/**
	 * Updates a slot with the given allocation id.
	 *
	 * @param slotId to update
	 * @param allocationId specifying the current allocation of the slot
	 */
	private void updateSlot(SlotID slotId, AllocationID allocationId) {
		TaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			// we assume the given allocation id to be the ground truth (coming from the TM)
			slot.setAllocationId(allocationId);

			if (null != allocationId) {
				if (slot.hasPendingSlotRequest()){
					// we have a pending slot request --> check whether we have to reject it
					PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

					if (Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {
						// we can cancel the slot request because it has been fulfilled
						cancelPendingSlotRequest(pendingSlotRequest);

						// remove the pending slot request, since it has been completed
						pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());
					} else {
						// this will try to find a new slot for the request
						rejectPendingSlotRequest(
							pendingSlotRequest,
							new Exception("Task manager reported slot " + slotId + " being already allocated."));
					}

					slot.setAssignedSlotRequest(null);
				}

				fulfilledSlotRequests.put(allocationId, slotId);

				TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

				if (null != taskManagerRegistration) {
					// disable any registered time out for the task manager
					taskManagerRegistration.cancelTimeout();
				}
			}
		} else {
			LOG.debug("Trying to update unknown slot with slot id {}.", slotId);
		}
	}

	/**
	 * Tries to allocate a slot for the given slot request. If there is no slot available, the
	 * resource manager is informed to allocate more resources and a timeout for the request is
	 * registered.
	 *
	 * @param pendingSlotRequest to allocate a slot for
	 * @throws ResourceManagerException if the resource manager cannot allocate more resource
	 */
	private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
		TaskManagerSlot taskManagerSlot = findMatchingSlot(pendingSlotRequest.getResourceProfile());

		if (taskManagerSlot != null) {
			allocateSlot(taskManagerSlot, pendingSlotRequest);
		} else {
			final UUID timeoutIdentifier = UUID.randomUUID();
			final AllocationID allocationId = pendingSlotRequest.getAllocationId();

			// register timeout for slot request
			ScheduledFuture<?> timeoutFuture = scheduledExecutor.schedule(new Runnable() {
				@Override
				public void run() {
					mainThreadExecutor.execute(new Runnable() {
						@Override
						public void run() {
							timeoutSlotRequest(allocationId, timeoutIdentifier);
						}
					});
				}
			}, slotRequestTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			pendingSlotRequest.registerTimeout(timeoutFuture, timeoutIdentifier);

			resourceManagerActions.allocateResource(pendingSlotRequest.getResourceProfile());
		}
	}

	/**
	 * Allocates the given slot for the given slot request. This entails sending a registration
	 * message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot to allocate for the given slot request
	 * @param pendingSlotRequest to allocate the given slot for
	 */
	private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new FlinkCompletableFuture<>();
		final AllocationID allocationId = pendingSlotRequest.getAllocationId();
		final SlotID slotId = taskManagerSlot.getSlotId();

		taskManagerSlot.setAssignedSlotRequest(pendingSlotRequest);
		pendingSlotRequest.setRequestFuture(completableFuture);

		// RPC call to the task manager
		Future<Acknowledge> requestFuture = gateway.requestSlot(
			slotId,
			pendingSlotRequest.getJobId(),
			allocationId,
			pendingSlotRequest.getTargetAddress(),
			leaderId,
			taskManagerRequestTimeout);

		requestFuture.handle(new BiFunction<Acknowledge, Throwable, Void>() {
			@Override
			public Void apply(Acknowledge acknowledge, Throwable throwable) {
				if (acknowledge != null) {
					completableFuture.complete(acknowledge);
				} else {
					completableFuture.completeExceptionally(throwable);
				}

				return null;
			}
		});

		completableFuture.handleAsync(new BiFunction<Acknowledge, Throwable, Void>() {
			@Override
			public Void apply(Acknowledge acknowledge, Throwable throwable) {
				if (acknowledge != null) {
					updateSlot(slotId, allocationId);
				} else {
					if (throwable instanceof SlotOccupiedException) {
						SlotOccupiedException exception = (SlotOccupiedException) throwable;
						updateSlot(slotId, exception.getAllocationId());
					} else {
						removeSlotRequestFromSlot(slotId, allocationId);
					}

					if (!(throwable instanceof CancellationException)) {
						handleFailedSlotRequest(slotId, allocationId, throwable);
					} else {
						LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
					}
				}

				return null;
			}
		}, mainThreadExecutor);
	}

	/**
	 * Handles a free slot. It first tries to find a pending slot request which can be fulfilled.
	 * If there is no such request, then it will add the slot to the set of free slots.
	 *
	 * @param freeSlot to find a new slot request for
	 */
	private void handleFreeSlot(TaskManagerSlot freeSlot) {
		PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());

		if (null != pendingSlotRequest) {
			allocateSlot(freeSlot, pendingSlotRequest);
		} else {
			freeSlots.put(freeSlot.getSlotId(), freeSlot);
		}
	}

	/**
	 * Removes the given set of slots from the slot manager.
	 *
	 * @param slotsToRemove identifying the slots to remove from the slot manager
	 */
	private void removeSlots(Iterable<SlotID> slotsToRemove) {
		for (SlotID slotId : slotsToRemove) {
			removeSlot(slotId);
		}
	}

	/**
	 * Removes the given slot from the slot manager.
	 *
	 * @param slotId identifying the slot to remove
	 */
	private void removeSlot(SlotID slotId) {
		TaskManagerSlot slot = slots.remove(slotId);

		if (null != slot) {
			freeSlots.remove(slotId);

			if (slot.hasPendingSlotRequest()) {
				// reject the pending slot request --> triggering a new allocation attempt
				rejectPendingSlotRequest(
					slot.getAssignedSlotRequest(),
					new Exception("The assigned slot " + slot.getSlotId() + " was removed."));
			}

			AllocationID oldAllocationId = slot.getAllocationId();

			fulfilledSlotRequests.remove(oldAllocationId);

			TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

			if (null != taskManagerRegistration) {
				taskManagerRegistration.removeSlot(slotId);
			}
		} else {
			LOG.debug("There was no slot registered with slot id {}.", slotId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal request handling methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Removes a pending slot request identified by the given allocation id from a slot identified
	 * by the given slot id.
	 *
	 * @param slotId identifying the slot
	 * @param allocationId identifying the presumable assigned pending slot request
	 */
	private void removeSlotRequestFromSlot(SlotID slotId, AllocationID allocationId) {
		TaskManagerSlot taskManagerSlot = slots.get(slotId);

		if (null != taskManagerSlot) {
			if (taskManagerSlot.hasPendingSlotRequest() && Objects.equals(allocationId, taskManagerSlot.getAssignedSlotRequest().getAllocationId())) {
				taskManagerSlot.setAssignedSlotRequest(null);
			}

			if (taskManagerSlot.isFree()) {
				handleFreeSlot(taskManagerSlot);
			}

			TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(taskManagerSlot.getInstanceId());

			if (null != taskManagerRegistration && !anySlotUsed(taskManagerRegistration.getSlots())) {
				registerTaskManagerTimeout(taskManagerRegistration);
			}
		} else {
			LOG.debug("There was no slot with {} registered. Probably this slot has been already freed.", slotId);
		}
	}

	/**
	 * Handles a failed slot request. The slot manager tries to find a new slot fulfilling
	 * the resource requirements for the failed slot request.
	 *
	 * @param slotId identifying the slot which was assigned to the slot request before
	 * @param allocationId identifying the failed slot request
	 * @param cause of the failure
	 */
	private void handleFailedSlotRequest(SlotID slotId, AllocationID allocationId, Throwable cause) {
		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.get(allocationId);

		LOG.debug("Slot request with allocation id {} failed for slot {}.", allocationId, slotId, cause);

		if (null != pendingSlotRequest) {
			pendingSlotRequest.setRequestFuture(null);

			try {
				internalRequestSlot(pendingSlotRequest);
			} catch (ResourceManagerException e) {
				pendingSlotRequests.remove(allocationId);

				resourceManagerActions.notifyAllocationFailure(
					pendingSlotRequest.getJobId(),
					allocationId,
					e);
			}
		} else {
			LOG.debug("There was not pending slot request with allocation id {}. Probably the request has been fulfilled or cancelled.", allocationId);
		}
	}

	/**
	 * Rejects the pending slot request by failing the request future with a
	 * {@link SlotAllocationException}.
	 *
	 * @param pendingSlotRequest to reject
	 * @param cause of the rejection
	 */
	private void rejectPendingSlotRequest(PendingSlotRequest pendingSlotRequest, Exception cause) {
		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

		if (null != request) {
			request.completeExceptionally(new SlotAllocationException(cause));
		} else {
			LOG.debug("Cannot reject pending slot request {}, since no request has been sent.", pendingSlotRequest.getAllocationId());
		}
	}

	/**
	 * Cancels the given slot request.
	 *
	 * @param pendingSlotRequest to cancel
	 */
	private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		pendingSlotRequest.cancelTimeout();

		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

		if (null != request) {
			request.cancel(false);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal timeout methods
	// ---------------------------------------------------------------------------------------------

	private void timeoutTaskManager(InstanceID instanceId, UUID timeoutIdentifier) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

		if (null != taskManagerRegistration) {
			if (Objects.equals(timeoutIdentifier, taskManagerRegistration.getTimeoutIdentifier())) {
				if (anySlotUsed(taskManagerRegistration.getSlots())) {
					LOG.debug("Cannot release the task manager with instance id {}, because some " +
						"of its slots are still being used.", instanceId);
				} else {
					unregisterTaskManager(instanceId);

					resourceManagerActions.releaseResource(instanceId);
				}
			} else {
				taskManagerRegistrations.put(instanceId, taskManagerRegistration);

				LOG.debug("Expected timeout identifier {} differs from the task manager's " +
					"timeout identifier {}. Ignoring the task manager timeout call.",
					timeoutIdentifier, taskManagerRegistration.getTimeoutIdentifier());
			}
		} else {
			LOG.debug("Could not find a registered task manager with instance id {}. Ignoring the task manager timeout call.", instanceId);
		}
	}

	private void timeoutSlotRequest(AllocationID allocationId, UUID timeoutIdentifier) {
		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

		if (null != pendingSlotRequest) {
			if (Objects.equals(timeoutIdentifier, pendingSlotRequest.getTimeoutIdentifier())) {
				if (!pendingSlotRequest.isAssigned()) {

					resourceManagerActions.notifyAllocationFailure(
						pendingSlotRequest.getJobId(),
						allocationId,
						new TimeoutException("The allocation could not be fulfilled in time."));
				} else {
					LOG.debug("Cannot fail pending slot request {} because it has been assigned.", allocationId);
				}
			} else {
				pendingSlotRequests.put(allocationId, pendingSlotRequest);

				LOG.debug("Expected timeout identifier {} differs from the pending slot request's " +
					"timeout identifier {}. Ignoring the slot request timeout call.",
					timeoutIdentifier, pendingSlotRequest.getTimeoutIdentifier());
			}
		} else {
			LOG.debug("Could not find pending slot request with allocation id {}. Ignoring the slot request timeout call.", allocationId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private boolean checkDuplicateRequest(AllocationID allocationId) {
		return pendingSlotRequests.containsKey(allocationId) || fulfilledSlotRequests.containsKey(allocationId);
	}

	private boolean anySlotUsed(Iterable<SlotID> slotsToCheck) {

		if (null != slotsToCheck) {
			boolean idle = true;

			for (SlotID slotId : slotsToCheck) {
				TaskManagerSlot taskManagerSlot = slots.get(slotId);

				if (null != taskManagerSlot) {
					idle &= taskManagerSlot.isFree();
				}
			}

			return !idle;
		} else {
			return false;
		}
	}

	private void registerTaskManagerTimeout(final TaskManagerRegistration taskManagerRegistration) {
		final UUID timeoutIdentifier = UUID.randomUUID();

		ScheduledFuture<?> timeoutFuture = scheduledExecutor.schedule(new Runnable() {
			@Override
			public void run() {
				mainThreadExecutor.execute(new Runnable() {
					@Override
					public void run() {
						timeoutTaskManager(taskManagerRegistration.getInstanceId(), timeoutIdentifier);
					}
				});
			}
		}, taskManagerTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		taskManagerRegistration.registerTimeout(timeoutFuture, timeoutIdentifier);
	}

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	TaskManagerSlot getSlot(SlotID slotId) {
		return slots.get(slotId);
	}

	@VisibleForTesting
	int getNumberRegisteredSlots() {
		return slots.size();
	}

	@VisibleForTesting
	PendingSlotRequest getSlotRequest(AllocationID allocationId) {
		return pendingSlotRequests.get(allocationId);
	}

	@VisibleForTesting
	boolean hasTimeoutRegistered(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {
			return taskManagerRegistration.getTimeoutIdentifier() != null;
		} else {
			return false;
		}
	}
}
