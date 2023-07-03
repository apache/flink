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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.MathUtils;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * SlotManager component for various task executor related responsibilities of the slot manager,
 * including:
 *
 * <ul>
 *   <li>tracking registered task executors
 *   <li>allocating new task executors (both on-demand, and for redundancy)
 *   <li>releasing idle task executors
 *   <li>tracking pending slots (expected slots from executors that are currently being allocated
 *   <li>tracking how many slots are used on each task executor
 * </ul>
 *
 * <p>Dev note: This component only exists to keep the code out of the slot manager. It covers many
 * aspects that aren't really the responsibility of the slot manager, and should be refactored to
 * live outside the slot manager and split into multiple parts.
 */
class TaskExecutorManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorManager.class);

    private final ResourceProfile defaultSlotResourceProfile;

    /** The default resource spec of workers to request. */
    private final WorkerResourceSpec defaultWorkerResourceSpec;

    private final int numSlotsPerWorker;

    /** Defines the max limitation of the total number of slots. */
    private final int maxSlotNum;

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    /** Defines the number of redundant taskmanagers. */
    private final int redundantTaskManagerNum;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Callbacks for resource (de-)allocations. */
    private final ResourceAllocator resourceAllocator;

    /** All currently registered task managers. */
    private final Map<InstanceID, TaskManagerRegistration> taskManagerRegistrations =
            new HashMap<>();

    private final Map<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots = new HashMap<>();

    private final Executor mainThreadExecutor;

    @Nullable private final ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

    private final Set<InstanceID> unWantedWorkers;
    private final ScheduledExecutor scheduledExecutor;
    private final Duration declareNeededResourceDelay;
    private CompletableFuture<Void> declareNeededResourceFuture;

    TaskExecutorManager(
            WorkerResourceSpec defaultWorkerResourceSpec,
            int numSlotsPerWorker,
            int maxNumSlots,
            boolean waitResultConsumedBeforeRelease,
            int redundantTaskManagerNum,
            Time taskManagerTimeout,
            Duration declareNeededResourceDelay,
            ScheduledExecutor scheduledExecutor,
            Executor mainThreadExecutor,
            ResourceAllocator resourceAllocator) {

        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.maxSlotNum = maxNumSlots;
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        this.taskManagerTimeout = taskManagerTimeout;
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        defaultWorkerResourceSpec, numSlotsPerWorker);
        this.scheduledExecutor = scheduledExecutor;
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        this.unWantedWorkers = new HashSet<>();
        this.resourceAllocator = Preconditions.checkNotNull(resourceAllocator);
        this.mainThreadExecutor = mainThreadExecutor;
        if (resourceAllocator.isSupported()) {
            taskManagerTimeoutsAndRedundancyCheck =
                    scheduledExecutor.scheduleWithFixedDelay(
                            () ->
                                    mainThreadExecutor.execute(
                                            this::checkTaskManagerTimeoutsAndRedundancy),
                            0L,
                            taskManagerTimeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        } else {
            taskManagerTimeoutsAndRedundancyCheck = null;
        }
    }

    @Override
    public void close() {
        if (taskManagerTimeoutsAndRedundancyCheck != null) {
            taskManagerTimeoutsAndRedundancyCheck.cancel(false);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor (un)registration
    // ---------------------------------------------------------------------------------------------

    public boolean isTaskManagerRegistered(InstanceID instanceId) {
        return taskManagerRegistrations.containsKey(instanceId);
    }

    public boolean registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
            LOG.info(
                    "The total number of slots exceeds the max limitation {}, could not register the excess task executor {}.",
                    maxSlotNum,
                    taskExecutorConnection.getInstanceID());
            return false;
        }

        TaskManagerRegistration taskManagerRegistration =
                new TaskManagerRegistration(
                        taskExecutorConnection,
                        StreamSupport.stream(initialSlotReport.spliterator(), false)
                                .map(SlotStatus::getSlotID)
                                .collect(Collectors.toList()),
                        totalResourceProfile,
                        defaultSlotResourceProfile);

        taskManagerRegistrations.put(
                taskExecutorConnection.getInstanceID(), taskManagerRegistration);

        // next register the new slots
        for (SlotStatus slotStatus : initialSlotReport) {
            if (slotStatus.getJobID() == null) {
                findAndRemoveExactlyMatchingPendingTaskManagerSlot(slotStatus.getResourceProfile());
            }
        }

        return true;
    }

    private boolean isMaxSlotNumExceededAfterRegistration(SlotReport initialSlotReport) {
        // check if the total number exceed before matching pending slot.
        if (!isMaxSlotNumExceededAfterAdding(initialSlotReport.getNumSlotStatus())) {
            return false;
        }

        // check if the total number exceed slots after consuming pending slot.
        return isMaxSlotNumExceededAfterAdding(getNumNonPendingReportedNewSlots(initialSlotReport));
    }

    private int getNumNonPendingReportedNewSlots(SlotReport slotReport) {
        final Set<TaskManagerSlotId> matchingPendingSlots = new HashSet<>();

        for (SlotStatus slotStatus : slotReport) {
            if (slotStatus.getAllocationID() != null) {
                // only empty registered slots can match pending slots
                continue;
            }

            for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
                if (!matchingPendingSlots.contains(pendingTaskManagerSlot.getTaskManagerSlotId())
                        && isPendingSlotExactlyMatchingResourceProfile(
                                pendingTaskManagerSlot, slotStatus.getResourceProfile())) {
                    matchingPendingSlots.add(pendingTaskManagerSlot.getTaskManagerSlotId());
                    break; // pendingTaskManagerSlot loop
                }
            }
        }
        return slotReport.getNumSlotStatus() - matchingPendingSlots.size();
    }

    private void findAndRemoveExactlyMatchingPendingTaskManagerSlot(
            ResourceProfile resourceProfile) {
        for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if (isPendingSlotExactlyMatchingResourceProfile(
                    pendingTaskManagerSlot, resourceProfile)) {
                pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
                return;
            }
        }
    }

    private boolean isPendingSlotExactlyMatchingResourceProfile(
            PendingTaskManagerSlot pendingTaskManagerSlot, ResourceProfile resourceProfile) {
        return pendingTaskManagerSlot.getResourceProfile().equals(resourceProfile);
    }

    public void unregisterTaskExecutor(InstanceID instanceId) {
        taskManagerRegistrations.remove(instanceId);
        unWantedWorkers.remove(instanceId);
    }

    public Collection<InstanceID> getTaskExecutors() {
        return new ArrayList<>(taskManagerRegistrations.keySet());
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor allocation
    // ---------------------------------------------------------------------------------------------

    /**
     * Tries to allocate a worker that can provide a slot with the given resource profile.
     *
     * @param requestedSlotResourceProfile desired slot profile
     * @return an upper bound resource requirement that can be fulfilled by the new worker, if one
     *     was allocated
     */
    public Optional<ResourceRequirement> allocateWorker(
            ResourceProfile requestedSlotResourceProfile) {
        if (!resourceAllocator.isSupported()) {
            // resource cannot be allocated
            return Optional.empty();
        }

        final int numRegisteredSlots = getNumberRegisteredSlots();
        final int numPendingSlots = getNumberPendingTaskManagerSlots();
        if (isMaxSlotNumExceededAfterAdding(numSlotsPerWorker)) {
            LOG.warn(
                    "Could not allocate {} more slots. The number of registered and pending slots is {}, while the maximum is {}.",
                    numSlotsPerWorker,
                    numPendingSlots + numRegisteredSlots,
                    maxSlotNum);
            return Optional.empty();
        }

        if (!defaultSlotResourceProfile.isMatching(requestedSlotResourceProfile)) {
            // requested resource profile is unfulfillable
            return Optional.empty();
        }

        for (int i = 0; i < numSlotsPerWorker; ++i) {
            PendingTaskManagerSlot pendingTaskManagerSlot =
                    new PendingTaskManagerSlot(defaultSlotResourceProfile);
            pendingSlots.put(pendingTaskManagerSlot.getTaskManagerSlotId(), pendingTaskManagerSlot);
        }

        declareNeededResourcesWithDelay();

        return Optional.of(
                ResourceRequirement.create(defaultSlotResourceProfile, numSlotsPerWorker));
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewSlot) {
        return getNumberRegisteredSlots() + getNumberPendingTaskManagerSlots() + numNewSlot
                > maxSlotNum;
    }

    private Collection<ResourceDeclaration> getResourceDeclaration() {
        final int pendingWorkerNum =
                MathUtils.divideRoundUp(getNumberPendingTaskManagerSlots(), numSlotsPerWorker);
        Set<InstanceID> neededRegisteredWorkers = new HashSet<>(taskManagerRegistrations.keySet());
        neededRegisteredWorkers.removeAll(unWantedWorkers);
        final int totalWorkerNum = pendingWorkerNum + neededRegisteredWorkers.size();

        return Collections.singleton(
                new ResourceDeclaration(
                        defaultWorkerResourceSpec, totalWorkerNum, new HashSet<>(unWantedWorkers)));
    }

    private void declareNeededResourcesWithDelay() {
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
        resourceAllocator.declareResourceNeeded(getResourceDeclaration());
    }

    @VisibleForTesting
    int getNumberPendingTaskManagerSlots() {
        return pendingSlots.size();
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor idleness / redundancy
    // ---------------------------------------------------------------------------------------------

    private void checkTaskManagerTimeoutsAndRedundancy() {
        if (!taskManagerRegistrations.isEmpty()) {
            long currentTime = System.currentTimeMillis();

            ArrayList<TaskManagerRegistration> timedOutTaskManagers =
                    new ArrayList<>(taskManagerRegistrations.size());

            // first retrieve the timed out TaskManagers
            for (TaskManagerRegistration taskManagerRegistration :
                    taskManagerRegistrations.values()) {
                if (currentTime - taskManagerRegistration.getIdleSince()
                        >= taskManagerTimeout.toMilliseconds()) {
                    // we collect the instance ids first in order to avoid concurrent modifications
                    // by the
                    // ResourceAllocator.releaseResource call
                    timedOutTaskManagers.add(taskManagerRegistration);
                }
            }

            int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - getNumberFreeSlots();
            if (slotsDiff > 0) {
                if (pendingSlots.isEmpty()) {
                    // Keep enough redundant taskManagers from time to time.
                    int requiredTaskManagers =
                            MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
                    allocateRedundantTaskManagers(requiredTaskManagers);
                } else {
                    LOG.debug(
                            "There are some pending slots, skip allocate redundant task manager and wait them fulfilled.");
                }
            } else {
                // second we trigger the release resource callback which can decide upon the
                // resource release
                int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
                releaseIdleTaskExecutors(
                        timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
            }
        }
    }

    private void allocateRedundantTaskManagers(int number) {
        LOG.debug("Allocating {} task executors for redundancy.", number);
        int allocatedNumber = allocateWorkers(number);
        if (number != allocatedNumber) {
            LOG.warn(
                    "Expect to allocate {} taskManagers. Actually allocate {} taskManagers.",
                    number,
                    allocatedNumber);
        }
    }

    /**
     * Allocate a number of workers based on the input param.
     *
     * @param workerNum the number of workers to allocate
     * @return the number of successfully allocated workers
     */
    private int allocateWorkers(int workerNum) {
        int allocatedWorkerNum = 0;
        for (int i = 0; i < workerNum; ++i) {
            if (allocateWorker(defaultSlotResourceProfile).isPresent()) {
                ++allocatedWorkerNum;
            } else {
                break;
            }
        }
        return allocatedWorkerNum;
    }

    private void releaseIdleTaskExecutors(
            ArrayList<TaskManagerRegistration> timedOutTaskManagers, int releaseNum) {
        for (int index = 0; index < releaseNum; ++index) {
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(timedOutTaskManagers.get(index));
            } else {
                releaseIdleTaskExecutor(timedOutTaskManagers.get(index).getInstanceId());
            }
        }
    }

    private void releaseIdleTaskExecutorIfPossible(
            TaskManagerRegistration taskManagerRegistration) {
        long idleSince = taskManagerRegistration.getIdleSince();
        taskManagerRegistration
                .getTaskManagerConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            InstanceID timedOutTaskManagerId =
                                    taskManagerRegistration.getInstanceId();
                            boolean stillIdle = idleSince == taskManagerRegistration.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(timedOutTaskManagerId);
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID timedOutTaskManagerId) {
        Preconditions.checkState(resourceAllocator.isSupported());
        LOG.debug(
                "Release TaskExecutor {} because it exceeded the idle timeout.",
                timedOutTaskManagerId);
        unWantedWorkers.add(timedOutTaskManagerId);
        declareNeededResourcesWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // slot / resource counts
    // ---------------------------------------------------------------------------------------------

    public ResourceProfile getTotalRegisteredResources() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getTotalResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getTotalRegisteredResourcesOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(TaskManagerRegistration::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    public ResourceProfile getTotalFreeResources() {
        return taskManagerRegistrations.values().stream()
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getTotalFreeResourcesOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .orElse(ResourceProfile.ZERO);
    }

    public Iterable<SlotID> getSlotsOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId).getSlots();
    }

    public int getNumberRegisteredSlots() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getNumberRegisteredSlots)
                .reduce(0, Integer::sum);
    }

    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberRegisteredSlots();
        } else {
            return 0;
        }
    }

    public int getNumberFreeSlots() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getNumberFreeSlots)
                .reduce(0, Integer::sum);
    }

    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberFreeSlots();
        } else {
            return 0;
        }
    }

    public Collection<PendingTaskManagerSlot> getPendingTaskManagerSlots() {
        return pendingSlots.values();
    }

    /**
     * remove unused pending task manager slots.
     *
     * @param unusedResourceCounter the count of unused resources.
     */
    public void removePendingTaskManagerSlots(ResourceCounter unusedResourceCounter) {
        if (!resourceAllocator.isSupported()) {
            return;
        }
        Preconditions.checkState(unusedResourceCounter.getResources().size() == 1);
        Preconditions.checkState(
                unusedResourceCounter.getResources().contains(defaultSlotResourceProfile));

        int wantedPendingSlotsNumber =
                pendingSlots.size()
                        - unusedResourceCounter.getResourceCount(defaultSlotResourceProfile);
        pendingSlots.entrySet().removeIf(ignore -> pendingSlots.size() > wantedPendingSlotsNumber);

        declareNeededResourcesWithDelay();
    }

    /** clear all pending task manager slots. */
    public void clearPendingTaskManagerSlots() {
        if (!resourceAllocator.isSupported()) {
            return;
        }
        if (!pendingSlots.isEmpty()) {
            this.pendingSlots.clear();
            declareNeededResourcesWithDelay();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor slot book-keeping
    // ---------------------------------------------------------------------------------------------

    public void occupySlot(InstanceID instanceId) {
        taskManagerRegistrations.get(instanceId).occupySlot();
    }

    public void freeSlot(InstanceID instanceId) {
        taskManagerRegistrations.get(instanceId).freeSlot();
    }

    public void markUsed(InstanceID instanceID) {
        taskManagerRegistrations.get(instanceID).markUsed();
    }
}
