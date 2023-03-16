/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
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
import java.util.stream.Stream;

/** Implementation of {@link TaskManagerTracker} supporting fine-grained resource management. */
public class FineGrainedTaskManagerTracker implements TaskManagerTracker {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedTaskManagerTracker.class);

    /** Map for allocated and pending slots. */
    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    /** All currently registered task managers. */
    private final Map<InstanceID, FineGrainedTaskManagerRegistration> taskManagerRegistrations;

    /** All unwanted task managers. */
    private final Map<InstanceID, WorkerResourceSpec> unWantedTaskManagers;

    private final Map<PendingTaskManagerId, PendingTaskManager> pendingTaskManagers;

    private final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
            pendingSlotAllocationRecords;

    private ResourceProfile totalRegisteredResource = ResourceProfile.ZERO;
    private ResourceProfile totalPendingResource = ResourceProfile.ZERO;
    private final ScheduledExecutor scheduledExecutor;
    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;
    /** Delay of the resource declare in the slot manager. */
    private final Duration declareNeededResourceDelay;
    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    private final CPUResource maxTotalCpu;
    private final MemorySize maxTotalMem;

    private boolean started;
    @Nullable private ResourceAllocator resourceAllocator;
    @Nullable private Executor mainThreadExecutor;

    @Nullable private CompletableFuture<Void> declareNeededResourceFuture;
    @Nullable private ScheduledFuture<?> taskManagerTimeoutsCheck;

    /**
     * Pending task manager indexed by the tuple of total resource profile and default slot resource
     * profile.
     */
    private final Map<Tuple2<ResourceProfile, ResourceProfile>, Set<PendingTaskManager>>
            totalAndDefaultSlotProfilesToPendingTaskManagers;

    public FineGrainedTaskManagerTracker(
            CPUResource maxTotalCpu,
            MemorySize maxTotalMem,
            boolean waitResultConsumedBeforeRelease,
            Time taskManagerTimeout,
            Duration declareNeededResourceDelay,
            ScheduledExecutor scheduledExecutor) {
        this.maxTotalCpu = maxTotalCpu;
        this.maxTotalMem = maxTotalMem;
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        this.taskManagerTimeout = taskManagerTimeout;
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        this.scheduledExecutor = scheduledExecutor;

        this.resourceAllocator = null;
        this.mainThreadExecutor = null;
        this.declareNeededResourceFuture = null;
        this.taskManagerTimeoutsCheck = null;

        slots = new HashMap<>();
        taskManagerRegistrations = new HashMap<>();
        unWantedTaskManagers = new HashMap<>();
        pendingTaskManagers = new HashMap<>();
        pendingSlotAllocationRecords = new HashMap<>();
        totalAndDefaultSlotProfilesToPendingTaskManagers = new HashMap<>();
    }

    @Override
    public void initialize(ResourceAllocator resourceAllocator, Executor mainThreadExecutor) {
        this.resourceAllocator = resourceAllocator;
        this.mainThreadExecutor = mainThreadExecutor;
        this.started = true;

        taskManagerTimeoutsCheck =
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> mainThreadExecutor.execute(this::checkTaskManagerTimeouts),
                        0L,
                        taskManagerTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        // stop the timeout checks for the TaskManagers
        if (taskManagerTimeoutsCheck != null) {
            taskManagerTimeoutsCheck.cancel(false);
            taskManagerTimeoutsCheck = null;
        }

        slots.clear();
        taskManagerRegistrations.clear();
        totalRegisteredResource = ResourceProfile.ZERO;
        pendingTaskManagers.clear();
        totalPendingResource = ResourceProfile.ZERO;
        pendingSlotAllocationRecords.clear();
        unWantedTaskManagers.clear();
        mainThreadExecutor = null;
        resourceAllocator = null;
        started = false;
    }

    private void replaceAllPendingAllocations(
            Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingSlotAllocations) {
        Preconditions.checkNotNull(pendingSlotAllocations);
        Preconditions.checkState(resourceAllocator.isSupported());
        LOG.trace("Record the pending allocations {}.", pendingSlotAllocations);
        pendingSlotAllocationRecords.clear();
        pendingSlotAllocationRecords.putAll(pendingSlotAllocations);
        removeUnusedPendingTaskManagers();
        declareNeededResourcesWithDelay();
    }

    @Override
    public void clearAllPendingAllocations() {
        checkInit();
        if (pendingSlotAllocationRecords.isEmpty()) {
            return;
        }
        Preconditions.checkState(resourceAllocator.isSupported());
        LOG.trace("clear all pending allocations.");
        pendingSlotAllocationRecords.clear();
        removeUnusedPendingTaskManagers();
        declareNeededResourcesWithDelay();
    }

    @Override
    public void clearPendingAllocationsOfJob(JobID jobId) {
        checkInit();
        if (pendingSlotAllocationRecords.isEmpty()) {
            return;
        }
        Preconditions.checkState(resourceAllocator.isSupported());
        LOG.info("Clear all pending allocations for job {}.", jobId);
        pendingSlotAllocationRecords.values().forEach(allocation -> allocation.remove(jobId));
        pendingSlotAllocationRecords
                .entrySet()
                .removeIf(
                        pendingTaskManagerAllocationRecord ->
                                pendingTaskManagerAllocationRecord.getValue().isEmpty());
        removeUnusedPendingTaskManagers();
        declareNeededResourcesWithDelay();
    }

    @Override
    public Set<PendingTaskManagerId> allocateTaskManagersAccordingTo(
            ResourceAllocationResult result) {
        checkInit();
        final Set<PendingTaskManagerId> failAllocations;
        if (resourceAllocator.isSupported()) {
            // Allocate task managers according to the result
            failAllocations =
                    allocateTaskManagersAccordingTo(result.getPendingTaskManagersToAllocate());

            // Record slot allocation of pending task managers
            final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                    pendingResourceAllocationResult =
                            new HashMap<>(result.getAllocationsOnPendingResources());
            pendingResourceAllocationResult.keySet().removeAll(failAllocations);
            replaceAllPendingAllocations(pendingResourceAllocationResult);
        } else {
            failAllocations =
                    result.getPendingTaskManagersToAllocate().stream()
                            .map(PendingTaskManager::getPendingTaskManagerId)
                            .collect(Collectors.toSet());
        }
        return failAllocations;
    }

    @Override
    public boolean registerTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile,
            PendingTaskManagerId matchedPendingTaskManagerId) {
        checkInit();
        Preconditions.checkNotNull(taskExecutorConnection);
        Preconditions.checkNotNull(totalResourceProfile);
        Preconditions.checkNotNull(defaultSlotResourceProfile);

        if (matchedPendingTaskManagerId == null
                && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {

            LOG.info(
                    "Can not register task manager {}. The max total resource limitation <{}, {}> is reached.",
                    taskExecutorConnection.getResourceID(),
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }

        LOG.debug(
                "Add task manager {} with total resource {} and default slot resource {}.",
                taskExecutorConnection.getInstanceID(),
                totalResourceProfile,
                defaultSlotResourceProfile);
        final FineGrainedTaskManagerRegistration taskManagerRegistration =
                new FineGrainedTaskManagerRegistration(
                        taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);
        taskManagerRegistrations.put(
                taskExecutorConnection.getInstanceID(), taskManagerRegistration);
        totalRegisteredResource = totalRegisteredResource.merge(totalResourceProfile);

        if (matchedPendingTaskManagerId != null) {
            removePendingTaskManager(matchedPendingTaskManagerId);
        }

        return true;
    }

    @Override
    public void unregisterTaskManager(InstanceID instanceId) {
        checkInit();
        Preconditions.checkNotNull(instanceId);
        unWantedTaskManagers.remove(instanceId);
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.remove(instanceId));
        totalRegisteredResource = totalRegisteredResource.subtract(taskManager.getTotalResource());
        LOG.debug("Remove task manager {}.", instanceId);
        for (AllocationID allocationId : taskManager.getAllocatedSlots().keySet()) {
            slots.remove(allocationId);
        }
    }

    private void addPendingTaskManager(PendingTaskManager pendingTaskManager) {
        Preconditions.checkNotNull(pendingTaskManager);
        LOG.debug("Add pending task manager {}.", pendingTaskManager);
        pendingTaskManagers.put(pendingTaskManager.getPendingTaskManagerId(), pendingTaskManager);
        totalPendingResource =
                totalPendingResource.merge(pendingTaskManager.getTotalResourceProfile());
        totalAndDefaultSlotProfilesToPendingTaskManagers
                .computeIfAbsent(
                        Tuple2.of(
                                pendingTaskManager.getTotalResourceProfile(),
                                pendingTaskManager.getDefaultSlotResourceProfile()),
                        ignored -> new HashSet<>())
                .add(pendingTaskManager);
    }

    private void removePendingTaskManager(PendingTaskManagerId pendingTaskManagerId) {
        Preconditions.checkNotNull(pendingTaskManagerId);
        final PendingTaskManager pendingTaskManager =
                Preconditions.checkNotNull(pendingTaskManagers.remove(pendingTaskManagerId));
        totalPendingResource =
                totalPendingResource.subtract(pendingTaskManager.getTotalResourceProfile());
        LOG.debug("Remove pending task manager {}.", pendingTaskManagerId);
        totalAndDefaultSlotProfilesToPendingTaskManagers.compute(
                Tuple2.of(
                        pendingTaskManager.getTotalResourceProfile(),
                        pendingTaskManager.getDefaultSlotResourceProfile()),
                (ignored, pendingTMSet) -> {
                    Preconditions.checkNotNull(pendingTMSet).remove(pendingTaskManager);
                    return pendingTMSet.isEmpty() ? null : pendingTMSet;
                });
        pendingSlotAllocationRecords.remove(pendingTaskManagerId);
    }

    @Override
    public Collection<TaskManagerInfo> getTaskManagersWithAllocatedSlotsForJob(JobID jobId) {
        return taskManagerRegistrations.values().stream()
                .filter(
                        taskManager ->
                                taskManager.getAllocatedSlots().values().stream()
                                        .anyMatch(slot -> jobId.equals(slot.getJobId())))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Core state transitions
    // ---------------------------------------------------------------------------------------------

    @Override
    public void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState slotState) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(instanceId);
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkNotNull(slotState);
        switch (slotState) {
            case FREE:
                freeSlot(instanceId, allocationId);
                break;
            case ALLOCATED:
                addAllocatedSlot(allocationId, jobId, instanceId, resourceProfile);
                break;
            case PENDING:
                addPendingSlot(allocationId, jobId, instanceId, resourceProfile);
                break;
        }
    }

    private void freeSlot(InstanceID instanceId, AllocationID allocationId) {
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        Preconditions.checkNotNull(slots.remove(allocationId));
        LOG.debug("Free allocated slot with allocationId {}.", allocationId);
        taskManager.freeSlot(allocationId);
    }

    private void addAllocatedSlot(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile) {
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        if (slots.containsKey(allocationId)) {
            // Complete allocation of pending slot
            LOG.debug("Complete slot allocation with allocationId {}.", allocationId);
            taskManager.notifyAllocationComplete(allocationId);
        } else {
            // New allocated slot
            LOG.debug("Register new allocated slot with allocationId {}.", allocationId);
            final FineGrainedTaskManagerSlot slot =
                    new FineGrainedTaskManagerSlot(
                            allocationId,
                            jobId,
                            resourceProfile,
                            taskManager.getTaskExecutorConnection(),
                            SlotState.ALLOCATED);
            slots.put(allocationId, slot);
            taskManager.notifyAllocation(allocationId, slot);
        }
    }

    private void addPendingSlot(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile) {
        Preconditions.checkState(!slots.containsKey(allocationId));
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        LOG.debug("Add pending slot with allocationId {}.", allocationId);
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        resourceProfile,
                        taskManager.getTaskExecutorConnection(),
                        SlotState.PENDING);
        taskManager.notifyAllocation(allocationId, slot);
        slots.put(allocationId, slot);
    }

    private void removeUnusedPendingTaskManagers() {
        Set<PendingTaskManagerId> unusedPendingTaskManager =
                pendingTaskManagers.keySet().stream()
                        .filter(id -> !pendingSlotAllocationRecords.containsKey(id))
                        .collect(Collectors.toSet());
        for (PendingTaskManagerId pendingTaskManagerId : unusedPendingTaskManager) {
            removePendingTaskManager(pendingTaskManagerId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Getters of internal state
    // ---------------------------------------------------------------------------------------------

    @Override
    public Map<JobID, ResourceCounter> getPendingAllocationsOfPendingTaskManager(
            PendingTaskManagerId pendingTaskManagerId) {
        return Collections.unmodifiableMap(
                pendingSlotAllocationRecords.getOrDefault(
                        pendingTaskManagerId, Collections.emptyMap()));
    }

    @Override
    public Collection<? extends TaskManagerInfo> getRegisteredTaskManagers() {
        return Collections.unmodifiableCollection(taskManagerRegistrations.values());
    }

    @Override
    public Optional<TaskManagerInfo> getRegisteredTaskManager(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId));
    }

    @Override
    public Optional<TaskManagerSlotInformation> getAllocatedOrPendingSlot(
            AllocationID allocationId) {
        return Optional.ofNullable(slots.get(allocationId));
    }

    @Override
    public Collection<PendingTaskManager> getPendingTaskManagers() {
        return Collections.unmodifiableCollection(pendingTaskManagers.values());
    }

    @Override
    public Collection<PendingTaskManager>
            getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                    ResourceProfile totalResourceProfile,
                    ResourceProfile defaultSlotResourceProfile) {
        return Collections.unmodifiableCollection(
                totalAndDefaultSlotProfilesToPendingTaskManagers.getOrDefault(
                        Tuple2.of(totalResourceProfile, defaultSlotResourceProfile),
                        Collections.emptySet()));
    }

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerRegistrations.values().stream()
                .mapToInt(TaskManagerInfo::getDefaultNumSlots)
                .sum();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getDefaultNumSlots)
                .orElse(0);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerRegistrations.keySet().stream()
                .mapToInt(this::getNumberFreeSlotsOf)
                .sum();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(
                        taskManager ->
                                Math.max(
                                        taskManager.getDefaultNumSlots()
                                                - taskManager.getAllocatedSlots().size(),
                                        0))
                .orElse(0);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return totalRegisteredResource;
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerInfo::getAvailableResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getAvailableResource)
                .orElse(ResourceProfile.ZERO);
    }

    @Override
    public ResourceProfile getPendingResource() {
        return totalPendingResource;
    }

    // ---------------------------------------------------------------------------------------------
    // Resource allocations.
    // ---------------------------------------------------------------------------------------------

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

    private boolean allocateResource(PendingTaskManager pendingTaskManager) {
        checkInit();
        Preconditions.checkState(resourceAllocator.isSupported());
        if (isMaxTotalResourceExceededAfterAdding(pendingTaskManager.getTotalResourceProfile())) {
            LOG.info(
                    "Could not allocate {}. Max total resource limitation <{}, {}> is reached.",
                    pendingTaskManager,
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }

        addPendingTaskManager(pendingTaskManager);
        return true;
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
        return getRegisteredTaskManagers().stream()
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
        checkInit();
        if (resourceAllocator.isSupported()) {
            addUnWantedTaskManager(timedOutTaskManagerId);
            declareNeededResourcesWithDelay();
        }
    }

    private void addUnWantedTaskManager(InstanceID instanceId) {
        final FineGrainedTaskManagerRegistration taskManager =
                taskManagerRegistrations.get(instanceId);
        if (taskManager != null) {
            unWantedTaskManagers.put(
                    instanceId,
                    WorkerResourceSpec.fromTotalResourceProfile(
                            taskManager.getTotalResource(),
                            SlotManagerUtils.calculateDefaultNumSlots(
                                    taskManager.getTotalResource(),
                                    taskManager.getDefaultSlotResourceProfile())));
        } else {
            LOG.debug("Unwanted task manager {} does not exists.", instanceId);
        }
    }

    void declareNeededResourcesWithDelay() {
        checkInit();
        Preconditions.checkState(resourceAllocator.isSupported());

        if (declareNeededResourceDelay.toMillis() <= 0) {
            declareNeededResources();
        } else {
            if (declareNeededResourceFuture == null || declareNeededResourceFuture.isDone()) {
                declareNeededResourceFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            declareNeededResources();
                                            Preconditions.checkNotNull(declareNeededResourceFuture)
                                                    .complete(null);
                                        }),
                        declareNeededResourceDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /** DO NOT call this method directly. Use {@link #declareNeededResourcesWithDelay()} instead. */
    private void declareNeededResources() {
        checkInit();
        Preconditions.checkState(resourceAllocator.isSupported());
        Map<WorkerResourceSpec, Set<InstanceID>> unWantedTaskManagerBySpec =
                unWantedTaskManagers.entrySet().stream()
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getValue,
                                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        // registered TaskManagers except unwanted worker.
        Stream<WorkerResourceSpec> registeredTaskManagerStream =
                getRegisteredTaskManagers().stream()
                        .filter(t -> !unWantedTaskManagers.containsKey(t.getInstanceId()))
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResource(), t.getDefaultNumSlots()));
        // pending TaskManagers.
        Stream<WorkerResourceSpec> pendingTaskManagerStream =
                getPendingTaskManagers().stream()
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResourceProfile(), t.getNumSlots()));

        Map<WorkerResourceSpec, Integer> requiredWorkers =
                Stream.concat(registeredTaskManagerStream, pendingTaskManagerStream)
                        .collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.summingInt(e -> 1)));

        Set<WorkerResourceSpec> workerResourceSpecs = new HashSet<>(requiredWorkers.keySet());
        workerResourceSpecs.addAll(unWantedTaskManagerBySpec.keySet());

        List<ResourceDeclaration> resourceDeclarations = new ArrayList<>();
        workerResourceSpecs.forEach(
                spec ->
                        resourceDeclarations.add(
                                new ResourceDeclaration(
                                        spec,
                                        requiredWorkers.getOrDefault(spec, 0),
                                        unWantedTaskManagerBySpec.getOrDefault(
                                                spec, Collections.emptySet()))));

        resourceAllocator.declareResourceNeeded(resourceDeclarations);
    }

    private boolean isMaxTotalResourceExceededAfterAdding(ResourceProfile newResource) {
        final ResourceProfile totalResourceAfterAdding =
                newResource.merge(getRegisteredResource()).merge(getPendingResource());
        return totalResourceAfterAdding.getCpuCores().compareTo(maxTotalCpu) > 0
                || totalResourceAfterAdding.getTotalMemory().compareTo(maxTotalMem) > 0;
    }

    private void checkInit() {
        Preconditions.checkState(started);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceAllocator);
    }
}
