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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Implementation of {@link TaskManagerTracker} supporting fine-grained resource management. */
public class FineGrainedTaskManagerTracker implements TaskManagerTracker {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedTaskManagerTracker.class);

    /** Map for allocated and pending slots. */
    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    /** All currently registered task managers. */
    private final Map<InstanceID, FineGrainedTaskManagerRegistration> taskManagerRegistrations;

    private final Map<PendingTaskManagerId, PendingTaskManager> pendingTaskManagers;

    private final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
            pendingSlotAllocationRecords;

    private ResourceProfile totalRegisteredResource = ResourceProfile.ZERO;
    private ResourceProfile totalPendingResource = ResourceProfile.ZERO;

    /**
     * Pending task manager indexed by the tuple of total resource profile and default slot resource
     * profile.
     */
    private final Map<Tuple2<ResourceProfile, ResourceProfile>, Set<PendingTaskManager>>
            totalAndDefaultSlotProfilesToPendingTaskManagers;

    public FineGrainedTaskManagerTracker() {
        slots = new HashMap<>();
        taskManagerRegistrations = new HashMap<>();
        pendingTaskManagers = new HashMap<>();
        pendingSlotAllocationRecords = new HashMap<>();
        totalAndDefaultSlotProfilesToPendingTaskManagers = new HashMap<>();
    }

    @Override
    public void replaceAllPendingAllocations(
            Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingSlotAllocations) {
        Preconditions.checkNotNull(pendingSlotAllocations);
        LOG.trace("Record the pending allocations {}.", pendingSlotAllocations);
        pendingSlotAllocationRecords.clear();
        pendingSlotAllocationRecords.putAll(pendingSlotAllocations);
    }

    @Override
    public void addTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        Preconditions.checkNotNull(taskExecutorConnection);
        Preconditions.checkNotNull(totalResourceProfile);
        Preconditions.checkNotNull(defaultSlotResourceProfile);
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
    }

    @Override
    public void removeTaskManager(InstanceID instanceId) {
        Preconditions.checkNotNull(instanceId);
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.remove(instanceId));
        totalRegisteredResource = totalRegisteredResource.subtract(taskManager.getTotalResource());
        LOG.debug("Remove task manager {}.", instanceId);
        for (AllocationID allocationId : taskManager.getAllocatedSlots().keySet()) {
            slots.remove(allocationId);
        }
    }

    @Override
    public void addPendingTaskManager(PendingTaskManager pendingTaskManager) {
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

    @Override
    public Map<JobID, ResourceCounter> removePendingTaskManager(
            PendingTaskManagerId pendingTaskManagerId) {
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
        return Optional.ofNullable(pendingSlotAllocationRecords.remove(pendingTaskManagerId))
                .orElse(Collections.emptyMap());
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

    @Override
    public void clear() {
        slots.clear();
        taskManagerRegistrations.clear();
        totalRegisteredResource = ResourceProfile.ZERO;
        pendingTaskManagers.clear();
        totalPendingResource = ResourceProfile.ZERO;
        pendingSlotAllocationRecords.clear();
    }
}
