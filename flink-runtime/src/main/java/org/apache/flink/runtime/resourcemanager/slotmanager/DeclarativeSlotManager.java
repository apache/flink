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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Implementation of {@link SlotManager} supporting declarative slot management.
 */
public class DeclarativeSlotManager implements SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotManager.class);

	private final SlotTracker slotTracker;
	private final ResourceTracker resourceTracker;
	private final BiFunction<Executor, ResourceActions, TaskExecutorManager> taskExecutorManagerFactory;
	@Nullable
	private TaskExecutorManager taskExecutorManager;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;

	private final SlotMatchingStrategy slotMatchingStrategy;

	private final SlotManagerMetricGroup slotManagerMetricGroup;

	private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();
	private final HashMap<SlotID, CompletableFuture<Acknowledge>> pendingSlotAllocationFutures;

	private boolean sendNotEnoughResourceNotifications = true;

	/** ResourceManager's id. */
	@Nullable
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	@Nullable
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations. */
	@Nullable
	private ResourceActions resourceActions;

	/** True iff the component has been started. */
	private boolean started;

	public DeclarativeSlotManager(
			ScheduledExecutor scheduledExecutor,
			SlotManagerConfiguration slotManagerConfiguration,
			SlotManagerMetricGroup slotManagerMetricGroup,
			ResourceTracker resourceTracker,
			SlotTracker slotTracker) {

		Preconditions.checkNotNull(slotManagerConfiguration);
		this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
		this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
		this.resourceTracker = Preconditions.checkNotNull(resourceTracker);

		pendingSlotAllocationFutures = new HashMap<>(16);

		this.slotTracker = Preconditions.checkNotNull(slotTracker);
		slotTracker.registerSlotStatusUpdateListener(createSlotStatusUpdateListener());

		slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();

		taskExecutorManagerFactory = (executor, resourceActions) -> new TaskExecutorManager(
			slotManagerConfiguration.getDefaultWorkerResourceSpec(),
			slotManagerConfiguration.getNumSlotsPerWorker(),
			slotManagerConfiguration.getMaxSlotNum(),
			slotManagerConfiguration.isWaitResultConsumedBeforeRelease(),
			slotManagerConfiguration.getRedundantTaskManagerNum(),
			slotManagerConfiguration.getTaskManagerTimeout(),
			scheduledExecutor,
			executor,
			resourceActions);

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		taskExecutorManager = null;

		started = false;
	}

	private SlotStatusUpdateListener createSlotStatusUpdateListener() {
		return (taskManagerSlot, previous, current, jobId) -> {
			if (previous == SlotState.PENDING) {
				cancelAllocationFuture(taskManagerSlot.getSlotId());
			}

			if (current == SlotState.PENDING) {
				resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
			}
			if (current == SlotState.FREE) {
				resourceTracker.notifyLostResource(jobId, taskManagerSlot.getResourceProfile());
			}

			if (current == SlotState.ALLOCATED) {
				taskExecutorManager.occupySlot(taskManagerSlot.getInstanceId());
			}
			if (previous == SlotState.ALLOCATED && current == SlotState.FREE) {
				taskExecutorManager.freeSlot(taskManagerSlot.getInstanceId());
			}
		};
	}

	private void cancelAllocationFuture(SlotID slotId) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = pendingSlotAllocationFutures.remove(slotId);
		// the future may be null if we are just re-playing the state transitions due to a slot report
		if (acknowledgeCompletableFuture != null) {
			acknowledgeCompletableFuture.cancel(false);
		}
	}

	@Override
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		// this sets up a grace period, e.g., when the cluster was started, to give task executors time to connect
		sendNotEnoughResourceNotifications = failUnfulfillableRequest;

		if (failUnfulfillableRequest) {
			checkResourceRequirements();
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	@Override
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the slot manager.");

		this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceActions = Preconditions.checkNotNull(newResourceActions);
		taskExecutorManager = taskExecutorManagerFactory.apply(newMainThreadExecutor, newResourceActions);

		started = true;

		registerSlotManagerMetrics();
	}

	private void registerSlotManagerMetrics() {
		slotManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_AVAILABLE,
			() -> (long) getNumberFreeSlots());
		slotManagerMetricGroup.gauge(
			MetricNames.TASK_SLOTS_TOTAL,
			() -> (long) getNumberRegisteredSlots());
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	@Override
	public void suspend() {
		if (!started) {
			return;
		}

		LOG.info("Suspending the slot manager.");

		resourceTracker.clear();
		if (taskExecutorManager != null) {
			taskExecutorManager.close();

			for (InstanceID registeredTaskManager : taskExecutorManager.getTaskExecutors()) {
				unregisterTaskManager(registeredTaskManager, new SlotManagerException("The slot manager is being suspended."));
			}
		}

		taskExecutorManager = null;
		resourceManagerId = null;
		resourceActions = null;
		started = false;
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the slot manager.");

		suspend();
		slotManagerMetricGroup.close();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	@Override
	public void processResourceRequirements(ResourceRequirements resourceRequirements) {
		checkInit();
		LOG.debug("Received resource requirements from job {}: {}", resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());

		if (resourceRequirements.getResourceRequirements().isEmpty()) {
			jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
		} else {
			jobMasterTargetAddresses.put(resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
		}
		resourceTracker.notifyResourceRequirements(resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
		checkResourceRequirements();
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 * @return True if the task manager has not been registered before and is registered successfully; otherwise false
	 */
	@Override
	public boolean registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();
		LOG.debug("Registering task executor {} under {} at the slot manager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskExecutorManager.isTaskManagerRegistered(taskExecutorConnection.getInstanceID())) {
			LOG.debug("Task executor {} was already registered.", taskExecutorConnection.getResourceID());
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
			return false;
		} else {
			if (!taskExecutorManager.registerTaskManager(taskExecutorConnection, initialSlotReport)) {
				LOG.debug("Task executor {} could not be registered.", taskExecutorConnection.getResourceID());
				return false;
			}

			// register the new slots
			for (SlotStatus slotStatus : initialSlotReport) {
				slotTracker.addSlot(
					slotStatus.getSlotID(),
					slotStatus.getResourceProfile(),
					taskExecutorConnection,
					slotStatus.getJobID());
			}

			checkResourceRequirements();
			return true;
		}
	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
		checkInit();

		LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

		if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
			slotTracker.removeSlots(taskExecutorManager.getSlotsOf(instanceId));
			taskExecutorManager.unregisterTaskExecutor(instanceId);
			checkResourceRequirements();

			return true;
		} else {
			LOG.debug("There is no task executor registered with instance ID {}. Ignoring this message.", instanceId);

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
	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		checkInit();

		LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

		if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
			slotTracker.notifySlotStatus(slotReport);
			checkResourceRequirements();
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
	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		checkInit();
		LOG.debug("Freeing slot {}.", slotId);

		slotTracker.notifyFree(slotId);
		checkResourceRequirements();
	}

	// ---------------------------------------------------------------------------------------------
	// Requirement matching
	// ---------------------------------------------------------------------------------------------

	/**
	 * Matches resource requirements against available resources. In a first round requirements are matched against free
	 * slot, and any match results in a slot allocation.
	 * The remaining unfulfilled requirements are matched against pending slots, allocating more workers if no matching
	 * pending slot could be found.
	 * If the requirements for a job could not be fulfilled then a notification is sent to the job master informing it
	 * as such.
	 *
	 * <p>Performance notes: At it's core this method loops, for each job, over all free/pending slots for each required slot, trying to
	 * find a matching slot. One should generally go in with the assumption that this runs in
	 * numberOfJobsRequiringResources * numberOfRequiredSlots * numberOfFreeOrPendingSlots.
	 * This is especially important when dealing with pending slots, as matches between requirements and pending slots
	 * are not persisted and recomputed on each call.
	 * This may required further refinements in the future; e.g., persisting the matches between requirements and pending slots,
	 * or not matching against pending slots at all.
	 *
	 * <p>When dealing with unspecific resource profiles (i.e., {@link ResourceProfile#ANY}/{@link ResourceProfile#UNKNOWN}),
	 * then the number of free/pending slots is not relevant because we only need exactly 1 comparison to determine whether
	 * a slot can be fulfilled or not, since they are all the same anyway.
	 *
	 * <p>When dealing with specific resource profiles things can be a lot worse, with the classical cases
	 * where either no matches are found, or only at the very end of the iteration.
	 * In the absolute worst case, with J jobs, requiring R slots each with a unique resource profile such each pair
	 * of these profiles is not matching, and S free/pending slots that don't fulfill any requirement, then this method
	 * does a total of J*R*S resource profile comparisons.
	 */
	private void checkResourceRequirements() {
		final Map<JobID, Collection<ResourceRequirement>> missingResources = resourceTracker.getMissingResources();
		if (missingResources.isEmpty()) {
			return;
		}

		final Map<JobID, ResourceCounter> unfulfilledRequirements = new LinkedHashMap<>();
		for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements : missingResources.entrySet()) {
			final JobID jobId = resourceRequirements.getKey();

			final ResourceCounter unfulfilledJobRequirements = tryAllocateSlotsForJob(jobId, resourceRequirements.getValue());
			if (!unfulfilledJobRequirements.isEmpty()) {
				unfulfilledRequirements.put(jobId, unfulfilledJobRequirements);
			}
		}
		if (unfulfilledRequirements.isEmpty()) {
			return;
		}

		final ResourceCounter pendingSlots = new ResourceCounter(taskExecutorManager.getPendingTaskManagerSlots().stream().collect(
			Collectors.groupingBy(
				PendingTaskManagerSlot::getResourceProfile,
				Collectors.summingInt(x -> 1))));

		for (Map.Entry<JobID, ResourceCounter> unfulfilledRequirement : unfulfilledRequirements.entrySet()) {
			tryFulfillRequirementsWithPendingSlots(
				unfulfilledRequirement.getKey(),
				unfulfilledRequirement.getValue().getResourceProfilesWithCount(),
				pendingSlots);
		}
	}

	private ResourceCounter tryAllocateSlotsForJob(JobID jobId, Collection<ResourceRequirement> missingResources) {
		final ResourceCounter outstandingRequirements = new ResourceCounter();

		for (ResourceRequirement resourceRequirement : missingResources) {
			int numMissingSlots = internalTryAllocateSlots(jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
			if (numMissingSlots > 0) {
				outstandingRequirements.incrementCount(resourceRequirement.getResourceProfile(), numMissingSlots);
			}
		}
		return outstandingRequirements;
	}

	/**
	 * Tries to allocate slots for the given requirement. If there are not enough slots available, the
	 * resource manager is informed to allocate more resources.
	 *
	 * @param jobId job to allocate slots for
	 * @param targetAddress address of the jobmaster
	 * @param resourceRequirement required slots
	 * @return the number of missing slots
	 */
	private int internalTryAllocateSlots(JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {
		final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();
		Collection<TaskManagerSlotInformation> freeSlots = slotTracker.getFreeSlots();

		int numUnfulfilled = 0;
		for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {

			final Optional<TaskManagerSlotInformation> reservedSlot = slotMatchingStrategy.findMatchingSlot(requiredResource, freeSlots, this::getNumberRegisteredSlotsOf);
			if (reservedSlot.isPresent()) {
				// we do not need to modify freeSlots because it is indirectly modified by the allocation
				allocateSlot(reservedSlot.get(), jobId, targetAddress, requiredResource);
			} else {
				// exit loop early; we won't find a matching slot for this requirement
				int numRemaining = resourceRequirement.getNumberOfRequiredSlots() - x;
				numUnfulfilled += numRemaining;
				break;
			}
		}
		return numUnfulfilled;
	}

	/**
	 * Allocates the given slot. This entails sending a registration message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot slot to allocate
	 * @param jobId job for which the slot should be allocated for
	 * @param targetAddress address of the job master
	 * @param resourceProfile resource profile for the requirement for which the slot is used
	 */
	private void allocateSlot(TaskManagerSlotInformation taskManagerSlot, JobID jobId, String targetAddress, ResourceProfile resourceProfile) {
		final SlotID slotId = taskManagerSlot.getSlotId();
		LOG.debug("Starting allocation of slot {} for job {} with resource profile {}.", slotId, jobId, resourceProfile);

		final InstanceID instanceId = taskManagerSlot.getInstanceId();
		if (!taskExecutorManager.isTaskManagerRegistered(instanceId)) {
			throw new IllegalStateException("Could not find a registered task manager for instance id " +
				instanceId + '.');
		}

		final TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		final TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();

		slotTracker.notifyAllocationStart(slotId, jobId);
		taskExecutorManager.markUsed(instanceId);
		pendingSlotAllocationFutures.put(slotId, completableFuture);

		// RPC call to the task manager
		CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
			slotId,
			jobId,
			new AllocationID(),
			resourceProfile,
			targetAddress,
			resourceManagerId,
			taskManagerRequestTimeout);

		requestFuture.whenComplete(
			(Acknowledge acknowledge, Throwable throwable) -> {
				if (acknowledge != null) {
					completableFuture.complete(acknowledge);
				} else {
					completableFuture.completeExceptionally(throwable);
				}
			});

		CompletableFuture<Void> slotAllocationResponseProcessingFuture = completableFuture.handleAsync(
			(Acknowledge acknowledge, Throwable throwable) -> {
				if (acknowledge != null) {
					LOG.trace("Completed allocation of slot {} for job {}.", slotId, jobId);
					slotTracker.notifyAllocationComplete(slotId, jobId);
				} else {
					if (throwable instanceof SlotOccupiedException) {
						SlotOccupiedException exception = (SlotOccupiedException) throwable;
						LOG.debug("Tried allocating slot {} for job {}, but it was already allocated for job {}.", slotId, jobId, exception.getJobId());
						// report as a slot status to force the state transition
						// this could be a problem if we ever assume that the task executor always reports about all slots
						slotTracker.notifySlotStatus(Collections.singleton(new SlotStatus(slotId, taskManagerSlot.getResourceProfile(), exception.getJobId(), exception.getAllocationId())));
					} else {
						if (throwable instanceof CancellationException) {
							LOG.debug("Cancelled allocation of slot {} for job {}.", slotId, jobId, throwable);
						} else {
							LOG.warn("Slot allocation for slot {} for job {} failed.", slotId, jobId, throwable);
							slotTracker.notifyFree(slotId);
						}
					}
					checkResourceRequirements();
				}
				return null;
			},
			mainThreadExecutor);
		FutureUtils.assertNoException(slotAllocationResponseProcessingFuture);
	}

	private void tryFulfillRequirementsWithPendingSlots(JobID jobId, Map<ResourceProfile, Integer> missingResources, ResourceCounter pendingSlots) {
		for (Map.Entry<ResourceProfile, Integer> missingResource : missingResources.entrySet()) {
			ResourceProfile profile = missingResource.getKey();
			for (int i = 0; i < missingResource.getValue(); i++) {
				if (!tryFulfillWithPendingSlots(profile, pendingSlots)) {
					boolean couldAllocateWorkerAndReserveSlot = tryAllocateWorkerAndReserveSlot(profile, pendingSlots);
					if (!couldAllocateWorkerAndReserveSlot && sendNotEnoughResourceNotifications) {
						LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
						resourceActions.notifyNotEnoughResourcesAvailable(jobId, resourceTracker.getAcquiredResources(jobId));
						return;
					}
				}
			}
		}
	}

	private boolean tryFulfillWithPendingSlots(ResourceProfile resourceProfile, ResourceCounter pendingSlots) {
		Set<ResourceProfile> pendingSlotProfiles = pendingSlots.getResourceProfiles();

		// short-cut, pretty much only applicable to fine-grained resource management
		if (pendingSlotProfiles.contains(resourceProfile)) {
			pendingSlots.decrementCount(resourceProfile, 1);
			return true;
		}

		for (ResourceProfile pendingSlotProfile : pendingSlotProfiles) {
			if (pendingSlotProfile.isMatching(resourceProfile)) {
				pendingSlots.decrementCount(pendingSlotProfile, 1);
				return true;
			}
		}

		return false;
	}

	private boolean tryAllocateWorkerAndReserveSlot(ResourceProfile profile, ResourceCounter pendingSlots) {
		Optional<ResourceRequirement> newlyFulfillableRequirements = taskExecutorManager.allocateWorker(profile);
		if (newlyFulfillableRequirements.isPresent()) {
			ResourceRequirement newSlots = newlyFulfillableRequirements.get();
			// reserve one of the new slots
			if (newSlots.getNumberOfRequiredSlots() > 1) {
				pendingSlots.incrementCount(newSlots.getResourceProfile(), newSlots.getNumberOfRequiredSlots() - 1);
			}
			return true;
		} else {
			return false;
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Legacy APIs
	// ---------------------------------------------------------------------------------------------

	@Override
	public int getNumberRegisteredSlots() {
		return taskExecutorManager.getNumberRegisteredSlots();
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		return taskExecutorManager.getNumberRegisteredSlotsOf(instanceId);
	}

	@Override
	public int getNumberFreeSlots() {
		return taskExecutorManager.getNumberFreeSlots();
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		return taskExecutorManager.getNumberFreeSlotsOf(instanceId);
	}

	@Override
	public Map<WorkerResourceSpec, Integer> getRequiredResources() {
		return taskExecutorManager.getRequiredWorkers();
	}

	@Override
	public ResourceProfile getRegisteredResource() {
		return taskExecutorManager.getTotalRegisteredResources();
	}

	@Override
	public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
		return taskExecutorManager.getTotalRegisteredResourcesOf(instanceID);
	}

	@Override
	public ResourceProfile getFreeResource() {
		return taskExecutorManager.getTotalFreeResources();
	}

	@Override
	public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
		return taskExecutorManager.getTotalFreeResourcesOf(instanceID);
	}

	@Override
	public int getNumberPendingSlotRequests() {
		// only exists for testing purposes
		throw new UnsupportedOperationException();
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}
}
