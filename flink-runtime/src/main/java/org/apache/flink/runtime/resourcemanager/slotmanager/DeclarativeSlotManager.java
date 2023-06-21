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
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Implementation of {@link SlotManager} supporting declarative slot management. */
public class DeclarativeSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotManager.class);

    private final SlotTracker slotTracker;
    private final ResourceTracker resourceTracker;
    private final BiFunction<Executor, ResourceAllocator, TaskExecutorManager>
            taskExecutorManagerFactory;
    @Nullable private TaskExecutorManager taskExecutorManager;

    /** Timeout for slot requests to the task manager. */
    private final Time taskManagerRequestTimeout;

    private final SlotMatchingStrategy slotMatchingStrategy;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();
    private final Map<SlotID, AllocationID> pendingSlotAllocations;

    /** Delay of the requirement change check in the slot manager. */
    private final Duration requirementsCheckDelay;

    private boolean sendNotEnoughResourceNotifications = true;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource not enough. */
    @Nullable private ResourceEventListener resourceEventListener;

    /** The future of the requirements delay check. */
    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    /** Blocked task manager checker. */
    @Nullable private BlockedTaskManagerChecker blockedTaskManagerChecker;

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
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
        this.requirementsCheckDelay = slotManagerConfiguration.getRequirementCheckDelay();

        pendingSlotAllocations = CollectionUtil.newHashMapWithExpectedSize(16);

        this.slotTracker = Preconditions.checkNotNull(slotTracker);
        slotTracker.registerSlotStatusUpdateListener(createSlotStatusUpdateListener());

        slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();

        taskExecutorManagerFactory =
                (executor, resourceAllocator) ->
                        new TaskExecutorManager(
                                slotManagerConfiguration.getDefaultWorkerResourceSpec(),
                                slotManagerConfiguration.getNumSlotsPerWorker(),
                                slotManagerConfiguration.getMaxSlotNum(),
                                slotManagerConfiguration.isWaitResultConsumedBeforeRelease(),
                                slotManagerConfiguration.getRedundantTaskManagerNum(),
                                slotManagerConfiguration.getTaskManagerTimeout(),
                                slotManagerConfiguration.getDeclareNeededResourceDelay(),
                                scheduledExecutor,
                                executor,
                                resourceAllocator);

        resourceManagerId = null;
        resourceEventListener = null;
        mainThreadExecutor = null;
        taskExecutorManager = null;
        blockedTaskManagerChecker = null;

        started = false;
    }

    private SlotStatusUpdateListener createSlotStatusUpdateListener() {
        return (taskManagerSlot, previous, current, jobId) -> {
            if (previous == SlotState.PENDING) {
                pendingSlotAllocations.remove(taskManagerSlot.getSlotId());
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

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest) {
            checkResourceRequirementsWithDelay();
        }
    }

    @Override
    public void triggerResourceRequirementsCheck() {
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator to use for resource (de-)allocations
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener newResourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {
        LOG.debug("Starting the slot manager.");

        this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceEventListener = Preconditions.checkNotNull(newResourceEventListener);
        taskExecutorManager =
                taskExecutorManagerFactory.apply(newMainThreadExecutor, newResourceAllocator);
        blockedTaskManagerChecker = Preconditions.checkNotNull(newBlockedTaskManagerChecker);

        started = true;

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /** Suspends the component. This clears the internal state of the slot manager. */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        slotManagerMetricGroup.close();

        resourceTracker.clear();
        if (taskExecutorManager != null) {
            taskExecutorManager.close();

            for (InstanceID registeredTaskManager : taskExecutorManager.getTaskExecutors()) {
                unregisterTaskManager(
                        registeredTaskManager,
                        new SlotManagerException("The slot manager is being suspended."));
            }
        }

        taskExecutorManager = null;
        resourceManagerId = null;
        resourceEventListener = null;
        blockedTaskManagerChecker = null;
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
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void clearResourceRequirements(JobID jobId) {
        checkInit();
        maybeReclaimInactiveSlots(jobId);
        jobMasterTargetAddresses.remove(jobId);
        resourceTracker.notifyResourceRequirements(jobId, Collections.emptyList());
    }

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        checkInit();
        if (resourceRequirements.getResourceRequirements().isEmpty()
                && resourceTracker.isRequirementEmpty(resourceRequirements.getJobId())) {
            return;
        } else if (resourceRequirements.getResourceRequirements().isEmpty()) {
            LOG.info("Clearing resource requirements of job {}", resourceRequirements.getJobId());
        } else {
            LOG.info(
                    "Received resource requirements from job {}: {}",
                    resourceRequirements.getJobId(),
                    resourceRequirements.getResourceRequirements());
        }

        if (!resourceRequirements.getResourceRequirements().isEmpty()) {
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirementsWithDelay();
    }

    private void maybeReclaimInactiveSlots(JobID jobId) {
        if (!resourceTracker.getAcquiredResources(jobId).isEmpty()) {
            final Collection<TaskExecutorConnection> taskExecutorsWithAllocatedSlots =
                    slotTracker.getTaskExecutorsWithAllocatedSlotsForJob(jobId);
            for (TaskExecutorConnection taskExecutorConnection : taskExecutorsWithAllocatedSlots) {
                final TaskExecutorGateway taskExecutorGateway =
                        taskExecutorConnection.getTaskExecutorGateway();
                taskExecutorGateway.freeInactiveSlots(jobId, taskManagerRequestTimeout);
            }
        }
    }

    @Override
    public RegistrationResult registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.debug(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskExecutorManager.isTaskManagerRegistered(taskExecutorConnection.getInstanceID())) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return RegistrationResult.IGNORED;
        } else {
            if (!taskExecutorManager.registerTaskManager(
                    taskExecutorConnection,
                    initialSlotReport,
                    totalResourceProfile,
                    defaultSlotResourceProfile)) {
                LOG.debug(
                        "Task executor {} could not be registered.",
                        taskExecutorConnection.getResourceID());
                return RegistrationResult.REJECTED;
            }

            // register the new slots
            for (SlotStatus slotStatus : initialSlotReport) {
                slotTracker.addSlot(
                        slotStatus.getSlotID(),
                        slotStatus.getResourceProfile(),
                        taskExecutorConnection,
                        slotStatus.getJobID());
            }

            checkResourceRequirementsWithDelay();
            return RegistrationResult.SUCCESS;
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
            slotTracker.removeSlots(taskExecutorManager.getSlotsOf(instanceId));
            taskExecutorManager.unregisterTaskExecutor(instanceId);
            checkResourceRequirementsWithDelay();

            return true;
        } else {
            LOG.debug(
                    "There is no task executor registered with instance ID {}. Ignoring this message.",
                    instanceId);

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
            if (slotTracker.notifySlotStatus(slotReport)) {
                checkResourceRequirementsWithDelay();
            }
            return true;
        } else {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

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
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Depending on the implementation of {@link ResourceAllocationStrategy}, checking resource
     * requirements and potentially making a re-allocation can be heavy. In order to cover more
     * changes with each check, thus reduce the frequency of unnecessary re-allocations, the checks
     * are performed with a slight delay.
     */
    private void checkResourceRequirementsWithDelay() {
        if (requirementsCheckDelay.toMillis() <= 0) {
            checkResourceRequirements();
        } else {
            if (requirementsCheckFuture == null || requirementsCheckFuture.isDone()) {
                requirementsCheckFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            checkResourceRequirements();
                                            Preconditions.checkNotNull(requirementsCheckFuture)
                                                    .complete(null);
                                        }),
                        requirementsCheckDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Matches resource requirements against available resources. In a first round requirements are
     * matched against free slot, and any match results in a slot allocation. The remaining
     * unfulfilled requirements are matched against pending slots, allocating more workers if no
     * matching pending slot could be found. If the requirements for a job could not be fulfilled
     * then a notification is sent to the job master informing it as such.
     *
     * <p>Performance notes: At it's core this method loops, for each job, over all free/pending
     * slots for each required slot, trying to find a matching slot. One should generally go in with
     * the assumption that this runs in numberOfJobsRequiringResources * numberOfRequiredSlots *
     * numberOfFreeOrPendingSlots. This is especially important when dealing with pending slots, as
     * matches between requirements and pending slots are not persisted and recomputed on each call.
     * This may required further refinements in the future; e.g., persisting the matches between
     * requirements and pending slots, or not matching against pending slots at all.
     *
     * <p>When dealing with unspecific resource profiles (i.e., {@link ResourceProfile#ANY}/{@link
     * ResourceProfile#UNKNOWN}), then the number of free/pending slots is not relevant because we
     * only need exactly 1 comparison to determine whether a slot can be fulfilled or not, since
     * they are all the same anyway.
     *
     * <p>When dealing with specific resource profiles things can be a lot worse, with the classical
     * cases where either no matches are found, or only at the very end of the iteration. In the
     * absolute worst case, with J jobs, requiring R slots each with a unique resource profile such
     * each pair of these profiles is not matching, and S free/pending slots that don't fulfill any
     * requirement, then this method does a total of J*R*S resource profile comparisons.
     *
     * <p>DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()}
     * instead.
     */
    private void checkResourceRequirements() {
        final Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        if (missingResources.isEmpty()) {
            taskExecutorManager.clearPendingTaskManagerSlots();
            return;
        }

        final Map<JobID, ResourceCounter> unfulfilledRequirements = new LinkedHashMap<>();
        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            final ResourceCounter unfulfilledJobRequirements =
                    tryAllocateSlotsForJob(jobId, resourceRequirements.getValue());
            if (!unfulfilledJobRequirements.isEmpty()) {
                unfulfilledRequirements.put(jobId, unfulfilledJobRequirements);
            }
        }
        if (unfulfilledRequirements.isEmpty()) {
            return;
        }

        ResourceCounter freePendingSlots =
                ResourceCounter.withResources(
                        taskExecutorManager.getPendingTaskManagerSlots().stream()
                                .collect(
                                        Collectors.groupingBy(
                                                PendingTaskManagerSlot::getResourceProfile,
                                                Collectors.summingInt(x -> 1))));

        for (Map.Entry<JobID, ResourceCounter> unfulfilledRequirement :
                unfulfilledRequirements.entrySet()) {
            freePendingSlots =
                    tryFulfillRequirementsWithPendingSlots(
                            unfulfilledRequirement.getKey(),
                            unfulfilledRequirement.getValue().getResourcesWithCount(),
                            freePendingSlots);
        }

        if (!freePendingSlots.isEmpty()) {
            taskExecutorManager.removePendingTaskManagerSlots(freePendingSlots);
        }
    }

    private ResourceCounter tryAllocateSlotsForJob(
            JobID jobId, Collection<ResourceRequirement> missingResources) {
        ResourceCounter outstandingRequirements = ResourceCounter.empty();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingSlots =
                    internalTryAllocateSlots(
                            jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
            if (numMissingSlots > 0) {
                outstandingRequirements =
                        outstandingRequirements.add(
                                resourceRequirement.getResourceProfile(), numMissingSlots);
            }
        }
        return outstandingRequirements;
    }

    /**
     * Tries to allocate slots for the given requirement. If there are not enough slots available,
     * the resource manager is informed to allocate more resources.
     *
     * @param jobId job to allocate slots for
     * @param targetAddress address of the jobmaster
     * @param resourceRequirement required slots
     * @return the number of missing slots
     */
    private int internalTryAllocateSlots(
            JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {
        final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();
        // Use LinkedHashMap to retain the original order
        final Map<SlotID, TaskManagerSlotInformation> availableSlots = new LinkedHashMap<>();
        for (TaskManagerSlotInformation freeSlot : slotTracker.getFreeSlots()) {
            if (!isBlockedTaskManager(freeSlot.getTaskManagerConnection().getResourceID())) {
                availableSlots.put(freeSlot.getSlotId(), freeSlot);
            }
        }

        int numUnfulfilled = 0;
        for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {

            final Optional<TaskManagerSlotInformation> reservedSlot =
                    slotMatchingStrategy.findMatchingSlot(
                            requiredResource,
                            availableSlots.values(),
                            this::getNumberRegisteredSlotsOf);
            if (reservedSlot.isPresent()) {
                allocateSlot(reservedSlot.get(), jobId, targetAddress, requiredResource);
                availableSlots.remove(reservedSlot.get().getSlotId());
            } else {
                // exit loop early; we won't find a matching slot for this requirement
                int numRemaining = resourceRequirement.getNumberOfRequiredSlots() - x;
                numUnfulfilled += numRemaining;
                break;
            }
        }
        return numUnfulfilled;
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        Preconditions.checkNotNull(blockedTaskManagerChecker);
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }

    /**
     * Allocates the given slot. This entails sending a registration message to the task manager and
     * treating failures.
     *
     * @param taskManagerSlot slot to allocate
     * @param jobId job for which the slot should be allocated for
     * @param targetAddress address of the job master
     * @param resourceProfile resource profile for the requirement for which the slot is used
     */
    private void allocateSlot(
            TaskManagerSlotInformation taskManagerSlot,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile) {
        final SlotID slotId = taskManagerSlot.getSlotId();
        LOG.debug(
                "Starting allocation of slot {} for job {} with resource profile {}.",
                slotId,
                jobId,
                resourceProfile);

        final InstanceID instanceId = taskManagerSlot.getInstanceId();
        if (!taskExecutorManager.isTaskManagerRegistered(instanceId)) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceId + '.');
        }

        final TaskExecutorConnection taskExecutorConnection =
                taskManagerSlot.getTaskManagerConnection();
        final TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

        final AllocationID allocationId = new AllocationID();

        slotTracker.notifyAllocationStart(slotId, jobId);
        taskExecutorManager.markUsed(instanceId);
        pendingSlotAllocations.put(slotId, allocationId);

        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        slotId,
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId,
                        taskManagerRequestTimeout);

        CompletableFuture<Void> slotAllocationResponseProcessingFuture =
                requestFuture.handleAsync(
                        (Acknowledge acknowledge, Throwable throwable) -> {
                            final AllocationID currentAllocationForSlot =
                                    pendingSlotAllocations.get(slotId);
                            if (currentAllocationForSlot == null
                                    || !currentAllocationForSlot.equals(allocationId)) {
                                LOG.debug(
                                        "Ignoring slot allocation update from task executor {} for slot {} and job {}, because the allocation was already completed or cancelled.",
                                        instanceId,
                                        slotId,
                                        jobId);
                                return null;
                            }
                            if (acknowledge != null) {
                                LOG.trace(
                                        "Completed allocation of slot {} for job {}.",
                                        slotId,
                                        jobId);
                                slotTracker.notifyAllocationComplete(slotId, jobId);
                            } else {
                                if (throwable instanceof SlotOccupiedException) {
                                    SlotOccupiedException exception =
                                            (SlotOccupiedException) throwable;
                                    LOG.debug(
                                            "Tried allocating slot {} for job {}, but it was already allocated for job {}.",
                                            slotId,
                                            jobId,
                                            exception.getJobId());
                                    // report as a slot status to force the state transition
                                    // this could be a problem if we ever assume that the task
                                    // executor always reports about all slots
                                    slotTracker.notifySlotStatus(
                                            Collections.singleton(
                                                    new SlotStatus(
                                                            slotId,
                                                            taskManagerSlot.getResourceProfile(),
                                                            exception.getJobId(),
                                                            exception.getAllocationId())));
                                } else {
                                    LOG.warn(
                                            "Slot allocation for slot {} for job {} failed.",
                                            slotId,
                                            jobId,
                                            throwable);
                                    slotTracker.notifyFree(slotId);
                                }
                                checkResourceRequirementsWithDelay();
                            }
                            return null;
                        },
                        mainThreadExecutor);
        FutureUtils.assertNoException(slotAllocationResponseProcessingFuture);
    }

    private ResourceCounter tryFulfillRequirementsWithPendingSlots(
            JobID jobId,
            Collection<Map.Entry<ResourceProfile, Integer>> missingResources,
            ResourceCounter pendingSlots) {
        for (Map.Entry<ResourceProfile, Integer> missingResource : missingResources) {
            ResourceProfile profile = missingResource.getKey();
            for (int i = 0; i < missingResource.getValue(); i++) {
                final MatchingResult matchingResult =
                        tryFulfillWithPendingSlots(profile, pendingSlots);
                pendingSlots = matchingResult.getNewAvailableResources();
                if (!matchingResult.isSuccessfulMatching()) {
                    final WorkerAllocationResult allocationResult =
                            tryAllocateWorkerAndReserveSlot(profile, pendingSlots);
                    pendingSlots = allocationResult.getNewAvailableResources();
                    if (!allocationResult.isSuccessfulAllocating()
                            && sendNotEnoughResourceNotifications) {
                        LOG.warn(
                                "Could not fulfill resource requirements of job {}. Free slots: {}",
                                jobId,
                                slotTracker.getFreeSlots().size());
                        resourceEventListener.notEnoughResourceAvailable(
                                jobId, resourceTracker.getAcquiredResources(jobId));
                        return pendingSlots;
                    }
                }
            }
        }
        return pendingSlots;
    }

    private MatchingResult tryFulfillWithPendingSlots(
            ResourceProfile resourceProfile, ResourceCounter pendingSlots) {
        Set<ResourceProfile> pendingSlotProfiles = pendingSlots.getResources();

        // short-cut, pretty much only applicable to fine-grained resource management
        if (pendingSlotProfiles.contains(resourceProfile)) {
            pendingSlots = pendingSlots.subtract(resourceProfile, 1);
            return new MatchingResult(true, pendingSlots);
        }

        for (ResourceProfile pendingSlotProfile : pendingSlotProfiles) {
            if (pendingSlotProfile.isMatching(resourceProfile)) {
                pendingSlots = pendingSlots.subtract(pendingSlotProfile, 1);
                return new MatchingResult(true, pendingSlots);
            }
        }

        return new MatchingResult(false, pendingSlots);
    }

    private WorkerAllocationResult tryAllocateWorkerAndReserveSlot(
            ResourceProfile profile, ResourceCounter pendingSlots) {
        Optional<ResourceRequirement> newlyFulfillableRequirements =
                taskExecutorManager.allocateWorker(profile);
        if (newlyFulfillableRequirements.isPresent()) {
            ResourceRequirement newSlots = newlyFulfillableRequirements.get();
            // reserve one of the new slots
            if (newSlots.getNumberOfRequiredSlots() > 1) {
                pendingSlots =
                        pendingSlots.add(
                                newSlots.getResourceProfile(),
                                newSlots.getNumberOfRequiredSlots() - 1);
            }
            return new WorkerAllocationResult(true, pendingSlots);
        } else {
            return new WorkerAllocationResult(false, pendingSlots);
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
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        // This information is currently not supported for this slot manager.
        return Collections.emptyList();
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
    }

    private static class MatchingResult {
        private final boolean isSuccessfulMatching;
        private final ResourceCounter newAvailableResources;

        private MatchingResult(
                boolean isSuccessfulMatching, ResourceCounter newAvailableResources) {
            this.isSuccessfulMatching = isSuccessfulMatching;
            this.newAvailableResources = Preconditions.checkNotNull(newAvailableResources);
        }

        private ResourceCounter getNewAvailableResources() {
            return newAvailableResources;
        }

        private boolean isSuccessfulMatching() {
            return isSuccessfulMatching;
        }
    }

    private static class WorkerAllocationResult {
        private final boolean isSuccessfulAllocating;
        private final ResourceCounter newAvailableResources;

        private WorkerAllocationResult(
                boolean isSuccessfulAllocating, ResourceCounter newAvailableResources) {
            this.isSuccessfulAllocating = isSuccessfulAllocating;
            this.newAvailableResources = Preconditions.checkNotNull(newAvailableResources);
        }

        private ResourceCounter getNewAvailableResources() {
            return newAvailableResources;
        }

        private boolean isSuccessfulAllocating() {
            return isSuccessfulAllocating;
        }
    }
}
