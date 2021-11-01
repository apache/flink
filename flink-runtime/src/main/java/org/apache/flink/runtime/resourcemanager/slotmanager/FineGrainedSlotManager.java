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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Implementation of {@link SlotManager} supporting fine-grained resource management. */
public class FineGrainedSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    private final TaskManagerTracker taskManagerTracker;
    private final ResourceTracker resourceTracker;
    private final ResourceAllocationStrategy resourceAllocationStrategy;

    private final SlotStatusSyncer slotStatusSyncer;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Delay of the requirement change check in the slot manager. */
    private final Time requirementsCheckDelay;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    private final CPUResource maxTotalCpu;
    private final MemorySize maxTotalMem;

    private boolean sendNotEnoughResourceNotifications = true;

    private final Set<JobID> unfulfillableJobs = new HashSet<>();

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource (de-)allocations. */
    @Nullable private ResourceActions resourceActions;

    @Nullable private ScheduledFuture<?> taskManagerTimeoutsCheck;

    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    /** True iff the component has been started. */
    private boolean started;

    public FineGrainedSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            TaskManagerTracker taskManagerTracker,
            SlotStatusSyncer slotStatusSyncer,
            ResourceAllocationStrategy resourceAllocationStrategy,
            Time requirementCheckDelay) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.requirementsCheckDelay = Preconditions.checkNotNull(requirementCheckDelay);

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskManagerTracker = Preconditions.checkNotNull(taskManagerTracker);
        this.slotStatusSyncer = Preconditions.checkNotNull(slotStatusSyncer);
        this.resourceAllocationStrategy = Preconditions.checkNotNull(resourceAllocationStrategy);

        this.maxTotalCpu = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalCpu());
        this.maxTotalMem = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalMem());

        resourceManagerId = null;
        resourceActions = null;
        mainThreadExecutor = null;
        taskManagerTimeoutsCheck = null;
        requirementsCheckFuture = null;

        started = false;
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        checkInit();
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest && !unfulfillableJobs.isEmpty()) {
            for (JobID jobId : unfulfillableJobs) {
                resourceActions.notifyNotEnoughResourcesAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
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
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceActions newResourceActions) {
        LOG.info("Starting the slot manager.");

        resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceActions = Preconditions.checkNotNull(newResourceActions);
        slotStatusSyncer.initialize(
                taskManagerTracker, resourceTracker, resourceManagerId, mainThreadExecutor);

        started = true;

        taskManagerTimeoutsCheck =
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> mainThreadExecutor.execute(this::checkTaskManagerTimeouts),
                        0L,
                        taskManagerTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS);

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

        // stop the timeout checks for the TaskManagers
        if (taskManagerTimeoutsCheck != null) {
            taskManagerTimeoutsCheck.cancel(false);
            taskManagerTimeoutsCheck = null;
        }

        slotStatusSyncer.close();
        taskManagerTracker.clear();
        resourceTracker.clear();

        unfulfillableJobs.clear();
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
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void clearResourceRequirements(JobID jobId) {
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

        if (resourceRequirements.getResourceRequirements().isEmpty()) {
            jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
        } else {
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirementsWithDelay();
    }

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
     * @param totalResourceProfile of the new task manager
     * @param defaultSlotResourceProfile of the new task manager
     * @return True if the task manager has not been registered before and is registered
     *     successfully; otherwise false
     */
    @Override
    public boolean registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.info(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskManagerTracker
                .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                .isPresent()) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return false;
        } else {
            Optional<PendingTaskManagerId> matchedPendingTaskManagerOptional =
                    initialSlotReport.hasAllocatedSlot()
                            ? Optional.empty()
                            : findMatchingPendingTaskManager(
                                    totalResourceProfile, defaultSlotResourceProfile);

            if (!matchedPendingTaskManagerOptional.isPresent()
                    && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {
                LOG.info(
                        "Releasing task manager {}. The max total resource limitation <{}, {}> is reached.",
                        taskExecutorConnection.getResourceID(),
                        maxTotalCpu,
                        maxTotalMem.toHumanReadableString());
                resourceActions.releaseResource(
                        taskExecutorConnection.getInstanceID(),
                        new FlinkException("The max total resource limitation is reached."));
                return false;
            }

            taskManagerTracker.addTaskManager(
                    taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

            if (initialSlotReport.hasAllocatedSlot()) {
                slotStatusSyncer.reportSlotStatus(
                        taskExecutorConnection.getInstanceID(), initialSlotReport);
            }

            if (matchedPendingTaskManagerOptional.isPresent()) {
                PendingTaskManagerId pendingTaskManager = matchedPendingTaskManagerOptional.get();
                allocateSlotsForRegisteredPendingTaskManager(
                        pendingTaskManager, taskExecutorConnection.getInstanceID());
                taskManagerTracker.removePendingTaskManager(pendingTaskManager);
                return true;
            }

            checkResourceRequirementsWithDelay();
            return true;
        }
    }

    private void allocateSlotsForRegisteredPendingTaskManager(
            PendingTaskManagerId pendingTaskManagerId, InstanceID instanceId) {
        Map<JobID, Map<InstanceID, ResourceCounter>> allocations =
                taskManagerTracker.getPendingAllocationsOfPendingTaskManager(pendingTaskManagerId)
                        .entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (e) -> Collections.singletonMap(instanceId, e.getValue())));
        allocateSlotsAccordingTo(allocations);
    }

    private Optional<PendingTaskManagerId> findMatchingPendingTaskManager(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
        Collection<PendingTaskManager> matchedPendingTaskManagers =
                taskManagerTracker.getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                        totalResourceProfile, defaultSlotResourceProfile);

        Optional<PendingTaskManagerId> matchedPendingTaskManagerIdsWithAllocatedSlots =
                matchedPendingTaskManagers.stream()
                        .map(PendingTaskManager::getPendingTaskManagerId)
                        .filter(
                                (pendingTaskManagerId) ->
                                        !taskManagerTracker
                                                .getPendingAllocationsOfPendingTaskManager(
                                                        pendingTaskManagerId)
                                                .isEmpty())
                        .findAny();

        if (matchedPendingTaskManagerIdsWithAllocatedSlots.isPresent()) {
            return matchedPendingTaskManagerIdsWithAllocatedSlots;
        } else {
            return matchedPendingTaskManagers.stream()
                    .map(PendingTaskManager::getPendingTaskManagerId)
                    .findAny();
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.info("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            Set<AllocationID> allocatedSlots =
                    new HashSet<>(
                            taskManagerTracker
                                    .getRegisteredTaskManager(instanceId)
                                    .get()
                                    .getAllocatedSlots()
                                    .keySet());
            for (AllocationID allocationId : allocatedSlots) {
                slotStatusSyncer.freeSlot(allocationId);
            }
            taskManagerTracker.removeTaskManager(instanceId);
            if (!allocatedSlots.isEmpty()) {
                checkResourceRequirementsWithDelay();
            }

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

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            if (!slotStatusSyncer.reportSlotStatus(instanceId, slotReport)) {
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
     * allocation id, then the slot will be freed.
     *
     * @param slotId identifying the slot to free, will be ignored
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", allocationId);

        if (taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent()) {
            slotStatusSyncer.freeSlot(allocationId);
            checkResourceRequirementsWithDelay();
        } else {
            LOG.debug(
                    "Trying to free a slot {} which has not been allocated. Ignoring this message.",
                    allocationId);
        }
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
                    requirementsCheckDelay.toMilliseconds(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()} instead.
     */
    private void checkResourceRequirements() {
        if (!started) {
            return;
        }
        Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        if (missingResources.isEmpty()) {
            return;
        }

        LOG.info("Matching resource requirements against available resources.");
        missingResources =
                missingResources.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));

        final ResourceAllocationResult result =
                resourceAllocationStrategy.tryFulfillRequirements(
                        missingResources, taskManagerTracker);

        // Allocate slots according to the result
        allocateSlotsAccordingTo(result.getAllocationsOnRegisteredResources());

        // Allocate task managers according to the result
        final Set<PendingTaskManagerId> failAllocations =
                allocateTaskManagersAccordingTo(result.getPendingTaskManagersToAllocate());

        // Record slot allocation of pending task managers
        final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                pendingResourceAllocationResult =
                        new HashMap<>(result.getAllocationsOnPendingResources());
        pendingResourceAllocationResult.keySet().removeAll(failAllocations);
        taskManagerTracker.replaceAllPendingAllocations(pendingResourceAllocationResult);

        unfulfillableJobs.clear();
        unfulfillableJobs.addAll(result.getUnfulfillableJobs());
        for (PendingTaskManagerId pendingTaskManagerId : failAllocations) {
            unfulfillableJobs.addAll(
                    result.getAllocationsOnPendingResources().get(pendingTaskManagerId).keySet());
        }
        // Notify jobs that can not be fulfilled
        if (sendNotEnoughResourceNotifications) {
            for (JobID jobId : unfulfillableJobs) {
                LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                resourceActions.notifyNotEnoughResourcesAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }
    }

    private void allocateSlotsAccordingTo(Map<JobID, Map<InstanceID, ResourceCounter>> result) {
        final List<CompletableFuture<Void>> allocationFutures = new ArrayList<>();
        for (Map.Entry<JobID, Map<InstanceID, ResourceCounter>> jobEntry : result.entrySet()) {
            final JobID jobID = jobEntry.getKey();
            for (Map.Entry<InstanceID, ResourceCounter> tmEntry : jobEntry.getValue().entrySet()) {
                final InstanceID instanceID = tmEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> slotEntry :
                        tmEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < slotEntry.getValue(); ++i) {
                        allocationFutures.add(
                                slotStatusSyncer.allocateSlot(
                                        instanceID,
                                        jobID,
                                        jobMasterTargetAddresses.get(jobID),
                                        slotEntry.getKey()));
                    }
                }
            }
        }
        FutureUtils.combineAll(allocationFutures)
                .whenCompleteAsync(
                        (s, t) -> {
                            if (t != null) {
                                // If there is allocation failure, we need to trigger it again.
                                checkResourceRequirementsWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    /**
     * Allocate pending task managers, returns the ids of pending task managers that can not be
     * allocated.
     */
    private Set<PendingTaskManagerId> allocateTaskManagersAccordingTo(
            List<PendingTaskManager> pendingTaskManagers) {
        final Set<PendingTaskManagerId> failedAllocations = new HashSet<>();
        for (PendingTaskManager pendingTaskManager : pendingTaskManagers) {
            if (!allocateResource(pendingTaskManager)) {
                failedAllocations.add(pendingTaskManager.getPendingTaskManagerId());
            }
        }
        return failedAllocations;
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerTracker.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerTracker.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        return taskManagerTracker.getPendingTaskManagers().stream()
                .map(
                        pendingTaskManager ->
                                WorkerResourceSpec.fromTotalResourceProfile(
                                        pendingTaskManager.getTotalResourceProfile(),
                                        pendingTaskManager.getNumSlots()))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(e -> 1)));
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskManagerTracker.getRegisteredResource();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredResourceOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerTracker.getFreeResource();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getFreeResourceOf(instanceID);
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredTaskManager(instanceID)
                .map(TaskManagerInfo::getAllocatedSlots).map(Map::values)
                .orElse(Collections.emptyList()).stream()
                .map(slot -> new SlotInfo(slot.getJobId(), slot.getResourceProfile()))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    private void checkTaskManagerTimeouts() {
        for (TaskManagerInfo timeoutTaskManager : getTimeOutTaskManagers()) {
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(timeoutTaskManager);
            } else {
                releaseIdleTaskExecutor(timeoutTaskManager.getInstanceId());
            }
        }
    }

    private Collection<TaskManagerInfo> getTimeOutTaskManagers() {
        long currentTime = System.currentTimeMillis();
        return taskManagerTracker.getRegisteredTaskManagers().stream()
                .filter(
                        taskManager ->
                                taskManager.isIdle()
                                        && currentTime - taskManager.getIdleSince()
                                                >= taskManagerTimeout.toMilliseconds())
                .collect(Collectors.toList());
    }

    private void releaseIdleTaskExecutorIfPossible(TaskManagerInfo taskManagerInfo) {
        final long idleSince = taskManagerInfo.getIdleSince();
        taskManagerInfo
                .getTaskExecutorConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle = idleSince == taskManagerInfo.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(taskManagerInfo.getInstanceId());
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID timedOutTaskManagerId) {
        final FlinkException cause = new FlinkException("TaskManager exceeded the idle timeout.");
        resourceActions.releaseResource(timedOutTaskManagerId, cause);
    }

    private boolean allocateResource(PendingTaskManager pendingTaskManager) {
        if (isMaxTotalResourceExceededAfterAdding(pendingTaskManager.getTotalResourceProfile())) {
            LOG.info(
                    "Could not allocate {}. Max total resource limitation <{}, {}> is reached.",
                    pendingTaskManager,
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }

        if (!resourceActions.allocateResource(
                WorkerResourceSpec.fromTotalResourceProfile(
                        pendingTaskManager.getTotalResourceProfile(),
                        pendingTaskManager.getNumSlots()))) {
            // resource cannot be allocated
            return false;
        }

        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        return true;
    }

    @VisibleForTesting
    public long getTaskManagerIdleSince(InstanceID instanceId) {
        return taskManagerTracker
                .getRegisteredTaskManager(instanceId)
                .map(TaskManagerInfo::getIdleSince)
                .orElse(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
        Preconditions.checkNotNull(resourceManagerId);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceActions);
    }

    private boolean isMaxTotalResourceExceededAfterAdding(ResourceProfile newResource) {
        final ResourceProfile totalResourceAfterAdding =
                newResource
                        .merge(taskManagerTracker.getRegisteredResource())
                        .merge(taskManagerTracker.getPendingResource());
        return totalResourceAfterAdding.getCpuCores().compareTo(maxTotalCpu) > 0
                || totalResourceAfterAdding.getTotalMemory().compareTo(maxTotalMem) > 0;
    }
}
