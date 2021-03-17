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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Implementation of {@link SlotManager}. */
public class SlotManagerImpl implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(SlotManagerImpl.class);

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout for slot requests to the task manager. */
    private final Time taskManagerRequestTimeout;

    /** Timeout after which an allocation is discarded. */
    private final Time slotRequestTimeout;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Map for all registered slots. */
    private final HashMap<SlotID, TaskManagerSlot> slots;

    /** Index of all currently free slots. */
    private final LinkedHashMap<SlotID, TaskManagerSlot> freeSlots;

    /** All currently registered task managers. */
    private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

    /** Map of fulfilled and active allocations for request deduplication purposes. */
    private final HashMap<AllocationID, SlotID> fulfilledSlotRequests;

    /** Map of pending/unfulfilled slot allocation requests. */
    private final HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests;

    private final HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots;

    private final SlotMatchingStrategy slotMatchingStrategy;

    /** ResourceManager's id. */
    private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    private Executor mainThreadExecutor;

    /** Callbacks for resource (de-)allocations. */
    private ResourceActions resourceActions;

    private ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

    private ScheduledFuture<?> slotRequestTimeoutCheck;

    /** True iff the component has been started. */
    private boolean started;

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    /** Defines the max limitation of the total number of slots. */
    private final int maxSlotNum;

    /** Defines the number of redundant taskmanagers. */
    private final int redundantTaskManagerNum;

    /**
     * If true, fail unfulfillable slot requests immediately. Otherwise, allow unfulfillable request
     * to pend. A slot request is considered unfulfillable if it cannot be fulfilled by neither a
     * slot that is already registered (including allocated ones) nor a pending slot that the {@link
     * ResourceActions} can allocate.
     */
    private boolean failUnfulfillableRequest = true;

    /** The default resource spec of workers to request. */
    private final WorkerResourceSpec defaultWorkerResourceSpec;

    private final int numSlotsPerWorker;

    private final ResourceProfile defaultSlotResourceProfile;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    public SlotManagerImpl(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();
        this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
        this.slotRequestTimeout = slotManagerConfiguration.getSlotRequestTimeout();
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
        this.numSlotsPerWorker = slotManagerConfiguration.getNumSlotsPerWorker();
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        defaultWorkerResourceSpec, numSlotsPerWorker);
        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
        this.maxSlotNum = slotManagerConfiguration.getMaxSlotNum();
        this.redundantTaskManagerNum = slotManagerConfiguration.getRedundantTaskManagerNum();

        slots = new HashMap<>(16);
        freeSlots = new LinkedHashMap<>(16);
        taskManagerRegistrations = new HashMap<>(4);
        fulfilledSlotRequests = new HashMap<>(16);
        pendingSlotRequests = new HashMap<>(16);
        pendingSlots = new HashMap<>(16);

        resourceManagerId = null;
        resourceActions = null;
        mainThreadExecutor = null;
        taskManagerTimeoutsAndRedundancyCheck = null;
        slotRequestTimeoutCheck = null;

        started = false;
    }

    @Override
    public int getNumberRegisteredSlots() {
        return slots.size();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberRegisteredSlots();
        } else {
            return 0;
        }
    }

    @Override
    public int getNumberFreeSlots() {
        return freeSlots.size();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberFreeSlots();
        } else {
            return 0;
        }
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        final int pendingWorkerNum =
                MathUtils.divideRoundUp(pendingSlots.size(), numSlotsPerWorker);
        return pendingWorkerNum > 0
                ? Collections.singletonMap(defaultWorkerResourceSpec, pendingWorkerNum)
                : Collections.emptyMap();
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getTotalResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(TaskManagerRegistration::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerRegistrations.values().stream()
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .orElse(ResourceProfile.ZERO);
    }

    @VisibleForTesting
    public int getNumberPendingTaskManagerSlots() {
        return pendingSlots.size();
    }

    @Override
    public int getNumberPendingSlotRequests() {
        return pendingSlotRequests.size();
    }

    @VisibleForTesting
    public int getNumberAssignedPendingTaskManagerSlots() {
        return (int)
                pendingSlots.values().stream()
                        .filter(slot -> slot.getAssignedPendingSlotRequest() != null)
                        .count();
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
        LOG.info("Starting the SlotManager.");

        this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceActions = Preconditions.checkNotNull(newResourceActions);

        started = true;

        taskManagerTimeoutsAndRedundancyCheck =
                scheduledExecutor.scheduleWithFixedDelay(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> checkTaskManagerTimeoutsAndRedundancy()),
                        0L,
                        taskManagerTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS);

        slotRequestTimeoutCheck =
                scheduledExecutor.scheduleWithFixedDelay(
                        () -> mainThreadExecutor.execute(() -> checkSlotRequestTimeouts()),
                        0L,
                        slotRequestTimeout.toMilliseconds(),
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
        LOG.info("Suspending the SlotManager.");

        // stop the timeout checks for the TaskManagers and the SlotRequests
        if (taskManagerTimeoutsAndRedundancyCheck != null) {
            taskManagerTimeoutsAndRedundancyCheck.cancel(false);
            taskManagerTimeoutsAndRedundancyCheck = null;
        }

        if (slotRequestTimeoutCheck != null) {
            slotRequestTimeoutCheck.cancel(false);
            slotRequestTimeoutCheck = null;
        }

        for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
            cancelPendingSlotRequest(pendingSlotRequest);
        }

        pendingSlotRequests.clear();

        ArrayList<InstanceID> registeredTaskManagers =
                new ArrayList<>(taskManagerRegistrations.keySet());

        for (InstanceID registeredTaskManager : registeredTaskManagers) {
            unregisterTaskManager(
                    registeredTaskManager,
                    new SlotManagerException("The slot manager is being suspended."));
        }

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
        LOG.info("Closing the SlotManager.");

        suspend();
        slotManagerMetricGroup.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        // no-op; don't throw an UnsupportedOperationException here because there are code paths
        // where the resource
        // manager calls this method regardless of whether declarative resource management is used
        // or not
    }

    /**
     * Requests a slot with the respective resource profile.
     *
     * @param slotRequest specifying the requested slot specs
     * @return true if the slot request was registered; false if the request is a duplicate
     * @throws ResourceManagerException if the slot request failed (e.g. not enough resources left)
     */
    @Override
    public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
        checkInit();

        if (checkDuplicateRequest(slotRequest.getAllocationId())) {
            LOG.debug(
                    "Ignoring a duplicate slot request with allocation id {}.",
                    slotRequest.getAllocationId());

            return false;
        } else {
            PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);

            pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);

            try {
                internalRequestSlot(pendingSlotRequest);
            } catch (ResourceManagerException e) {
                // requesting the slot failed --> remove pending slot request
                pendingSlotRequests.remove(slotRequest.getAllocationId());

                throw new ResourceManagerException(
                        "Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
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
    @Override
    public boolean unregisterSlotRequest(AllocationID allocationId) {
        checkInit();

        PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

        if (null != pendingSlotRequest) {
            LOG.debug("Cancel slot request {}.", allocationId);

            cancelPendingSlotRequest(pendingSlotRequest);

            return true;
        } else {
            LOG.debug(
                    "No pending slot request with allocation id {} found. Ignoring unregistration request.",
                    allocationId);

            return false;
        }
    }

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
     * @param totalResourceProfile for the new task manager
     * @param defaultSlotResourceProfile for the new task manager
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

        LOG.debug(
                "Registering TaskManager {} under {} at the SlotManager.",
                taskExecutorConnection.getResourceID().getStringWithMetadata(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return false;
        } else {
            if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
                LOG.info(
                        "The total number of slots exceeds the max limitation {}, release the excess resource.",
                        maxSlotNum);
                resourceActions.releaseResource(
                        taskExecutorConnection.getInstanceID(),
                        new FlinkException(
                                "The total number of slots exceeds the max limitation."));
                return false;
            }

            // first register the TaskManager
            ArrayList<SlotID> reportedSlots = new ArrayList<>();

            for (SlotStatus slotStatus : initialSlotReport) {
                reportedSlots.add(slotStatus.getSlotID());
            }

            TaskManagerRegistration taskManagerRegistration =
                    new TaskManagerRegistration(
                            taskExecutorConnection,
                            reportedSlots,
                            totalResourceProfile,
                            defaultSlotResourceProfile);

            taskManagerRegistrations.put(
                    taskExecutorConnection.getInstanceID(), taskManagerRegistration);

            // next register the new slots
            for (SlotStatus slotStatus : initialSlotReport) {
                registerSlot(
                        slotStatus.getSlotID(),
                        slotStatus.getAllocationID(),
                        slotStatus.getJobID(),
                        slotStatus.getResourceProfile(),
                        taskExecutorConnection);
            }

            return true;
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregister TaskManager {} from the SlotManager.", instanceId);

        TaskManagerRegistration taskManagerRegistration =
                taskManagerRegistrations.remove(instanceId);

        if (null != taskManagerRegistration) {
            internalUnregisterTaskManager(taskManagerRegistration, cause);

            return true;
        } else {
            LOG.debug(
                    "There is no task manager registered with instance ID {}. Ignoring this message.",
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

        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (null != taskManagerRegistration) {
            LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

            for (SlotStatus slotStatus : slotReport) {
                updateSlot(
                        slotStatus.getSlotID(),
                        slotStatus.getAllocationID(),
                        slotStatus.getJobID());
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

        TaskManagerSlot slot = slots.get(slotId);

        if (null != slot) {
            if (slot.getState() == SlotState.ALLOCATED) {
                if (Objects.equals(allocationId, slot.getAllocationId())) {

                    TaskManagerRegistration taskManagerRegistration =
                            taskManagerRegistrations.get(slot.getInstanceId());

                    if (taskManagerRegistration == null) {
                        throw new IllegalStateException(
                                "Trying to free a slot from a TaskManager "
                                        + slot.getInstanceId()
                                        + " which has not been registered.");
                    }

                    updateSlotState(slot, taskManagerRegistration, null, null);
                } else {
                    LOG.debug(
                            "Received request to free slot {} with expected allocation id {}, "
                                    + "but actual allocation id {} differs. Ignoring the request.",
                            slotId,
                            allocationId,
                            slot.getAllocationId());
                }
            } else {
                LOG.debug("Slot {} has not been allocated.", allocationId);
            }
        } else {
            LOG.debug(
                    "Trying to free a slot {} which has not been registered. Ignoring this message.",
                    slotId);
        }
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        if (!this.failUnfulfillableRequest && failUnfulfillableRequest) {
            // fail unfulfillable pending requests
            Iterator<Map.Entry<AllocationID, PendingSlotRequest>> slotRequestIterator =
                    pendingSlotRequests.entrySet().iterator();
            while (slotRequestIterator.hasNext()) {
                PendingSlotRequest pendingSlotRequest = slotRequestIterator.next().getValue();
                if (pendingSlotRequest.getAssignedPendingTaskManagerSlot() != null) {
                    continue;
                }
                if (!isFulfillableByRegisteredOrPendingSlots(
                        pendingSlotRequest.getResourceProfile())) {
                    slotRequestIterator.remove();
                    resourceActions.notifyAllocationFailure(
                            pendingSlotRequest.getJobId(),
                            pendingSlotRequest.getAllocationId(),
                            new UnfulfillableSlotRequestException(
                                    pendingSlotRequest.getAllocationId(),
                                    pendingSlotRequest.getResourceProfile()));
                }
            }
        }
        this.failUnfulfillableRequest = failUnfulfillableRequest;
    }

    // ---------------------------------------------------------------------------------------------
    // Behaviour methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Finds a matching slot request for a given resource profile. If there is no such request, the
     * method returns null.
     *
     * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
     * request fulfillment, then you should override this method.
     *
     * @param slotResourceProfile defining the resources of an available slot
     * @return A matching slot request which can be deployed in a slot with the given resource
     *     profile. Null if there is no such slot request pending.
     */
    private PendingSlotRequest findMatchingRequest(ResourceProfile slotResourceProfile) {

        for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
            if (!pendingSlotRequest.isAssigned()
                    && slotResourceProfile.isMatching(pendingSlotRequest.getResourceProfile())) {
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
     * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
     * request fulfillment, then you should override this method.
     *
     * @param requestResourceProfile specifying the resource requirements for the a slot request
     * @return A matching slot which fulfills the given resource profile. {@link Optional#empty()}
     *     if there is no such slot available.
     */
    private Optional<TaskManagerSlot> findMatchingSlot(ResourceProfile requestResourceProfile) {
        final Optional<TaskManagerSlot> optionalMatchingSlot =
                slotMatchingStrategy.findMatchingSlot(
                        requestResourceProfile,
                        freeSlots.values(),
                        this::getNumberRegisteredSlotsOf);

        optionalMatchingSlot.ifPresent(
                taskManagerSlot -> {
                    // sanity check
                    Preconditions.checkState(
                            taskManagerSlot.getState() == SlotState.FREE,
                            "TaskManagerSlot %s is not in state FREE but %s.",
                            taskManagerSlot.getSlotId(),
                            taskManagerSlot.getState());

                    freeSlots.remove(taskManagerSlot.getSlotId());
                });

        return optionalMatchingSlot;
    }

    // ---------------------------------------------------------------------------------------------
    // Internal slot operations
    // ---------------------------------------------------------------------------------------------

    /**
     * Registers a slot for the given task manager at the slot manager. The slot is identified by
     * the given slot id. The given resource profile defines the available resources for the slot.
     * The task manager connection can be used to communicate with the task manager.
     *
     * @param slotId identifying the slot on the task manager
     * @param allocationId which is currently deployed in the slot
     * @param resourceProfile of the slot
     * @param taskManagerConnection to communicate with the remote task manager
     */
    private void registerSlot(
            SlotID slotId,
            AllocationID allocationId,
            JobID jobId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection) {

        if (slots.containsKey(slotId)) {
            // remove the old slot first
            removeSlot(
                    slotId,
                    new SlotManagerException(
                            String.format(
                                    "Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
                                    slotId)));
        }

        final TaskManagerSlot slot =
                createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);

        final PendingTaskManagerSlot pendingTaskManagerSlot;

        if (allocationId == null) {
            pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
        } else {
            pendingTaskManagerSlot = null;
        }

        if (pendingTaskManagerSlot == null) {
            updateSlot(slotId, allocationId, jobId);
        } else {
            pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
            final PendingSlotRequest assignedPendingSlotRequest =
                    pendingTaskManagerSlot.getAssignedPendingSlotRequest();

            if (assignedPendingSlotRequest == null) {
                handleFreeSlot(slot);
            } else {
                assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
                allocateSlot(slot, assignedPendingSlotRequest);
            }
        }
    }

    @Nonnull
    private TaskManagerSlot createAndRegisterTaskManagerSlot(
            SlotID slotId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection) {
        final TaskManagerSlot slot =
                new TaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
        slots.put(slotId, slot);
        return slot;
    }

    @Nullable
    private PendingTaskManagerSlot findExactlyMatchingPendingTaskManagerSlot(
            ResourceProfile resourceProfile) {
        for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if (isPendingSlotExactlyMatchingResourceProfile(
                    pendingTaskManagerSlot, resourceProfile)) {
                return pendingTaskManagerSlot;
            }
        }

        return null;
    }

    private boolean isPendingSlotExactlyMatchingResourceProfile(
            PendingTaskManagerSlot pendingTaskManagerSlot, ResourceProfile resourceProfile) {
        return pendingTaskManagerSlot.getResourceProfile().equals(resourceProfile);
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

    /**
     * Updates a slot with the given allocation id.
     *
     * @param slotId to update
     * @param allocationId specifying the current allocation of the slot
     * @param jobId specifying the job to which the slot is allocated
     * @return True if the slot could be updated; otherwise false
     */
    private boolean updateSlot(SlotID slotId, AllocationID allocationId, JobID jobId) {
        final TaskManagerSlot slot = slots.get(slotId);

        if (slot != null) {
            final TaskManagerRegistration taskManagerRegistration =
                    taskManagerRegistrations.get(slot.getInstanceId());

            if (taskManagerRegistration != null) {
                updateSlotState(slot, taskManagerRegistration, allocationId, jobId);

                return true;
            } else {
                throw new IllegalStateException(
                        "Trying to update a slot from a TaskManager "
                                + slot.getInstanceId()
                                + " which has not been registered.");
            }
        } else {
            LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

            return false;
        }
    }

    private void updateSlotState(
            TaskManagerSlot slot,
            TaskManagerRegistration taskManagerRegistration,
            @Nullable AllocationID allocationId,
            @Nullable JobID jobId) {
        if (null != allocationId) {
            switch (slot.getState()) {
                case PENDING:
                    // we have a pending slot request --> check whether we have to reject it
                    PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

                    if (Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {
                        // we can cancel the slot request because it has been fulfilled
                        cancelPendingSlotRequest(pendingSlotRequest);

                        // remove the pending slot request, since it has been completed
                        pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());

                        slot.completeAllocation(allocationId, jobId);
                    } else {
                        // we first have to free the slot in order to set a new allocationId
                        slot.clearPendingSlotRequest();
                        // set the allocation id such that the slot won't be considered for the
                        // pending slot request
                        slot.updateAllocation(allocationId, jobId);

                        // remove the pending request if any as it has been assigned
                        final PendingSlotRequest actualPendingSlotRequest =
                                pendingSlotRequests.remove(allocationId);

                        if (actualPendingSlotRequest != null) {
                            cancelPendingSlotRequest(actualPendingSlotRequest);
                        }

                        // this will try to find a new slot for the request
                        rejectPendingSlotRequest(
                                pendingSlotRequest,
                                new Exception(
                                        "Task manager reported slot "
                                                + slot.getSlotId()
                                                + " being already allocated."));
                    }

                    taskManagerRegistration.occupySlot();
                    break;
                case ALLOCATED:
                    if (!Objects.equals(allocationId, slot.getAllocationId())) {
                        slot.freeSlot();
                        slot.updateAllocation(allocationId, jobId);
                    }
                    break;
                case FREE:
                    // the slot is currently free --> it is stored in freeSlots
                    freeSlots.remove(slot.getSlotId());
                    slot.updateAllocation(allocationId, jobId);
                    taskManagerRegistration.occupySlot();
                    break;
            }

            fulfilledSlotRequests.put(allocationId, slot.getSlotId());
        } else {
            // no allocation reported
            switch (slot.getState()) {
                case FREE:
                    handleFreeSlot(slot);
                    break;
                case PENDING:
                    // don't do anything because we still have a pending slot request
                    break;
                case ALLOCATED:
                    AllocationID oldAllocation = slot.getAllocationId();
                    slot.freeSlot();
                    fulfilledSlotRequests.remove(oldAllocation);
                    taskManagerRegistration.freeSlot();

                    handleFreeSlot(slot);
                    break;
            }
        }
    }

    /**
     * Tries to allocate a slot for the given slot request. If there is no slot available, the
     * resource manager is informed to allocate more resources and a timeout for the request is
     * registered.
     *
     * @param pendingSlotRequest to allocate a slot for
     * @throws ResourceManagerException if the slot request failed or is unfulfillable
     */
    private void internalRequestSlot(PendingSlotRequest pendingSlotRequest)
            throws ResourceManagerException {
        final ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();

        OptionalConsumer.of(findMatchingSlot(resourceProfile))
                .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
                .ifNotPresent(
                        () ->
                                fulfillPendingSlotRequestWithPendingTaskManagerSlot(
                                        pendingSlotRequest));
    }

    private void fulfillPendingSlotRequestWithPendingTaskManagerSlot(
            PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
        ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();
        Optional<PendingTaskManagerSlot> pendingTaskManagerSlotOptional =
                findFreeMatchingPendingTaskManagerSlot(resourceProfile);

        if (!pendingTaskManagerSlotOptional.isPresent()) {
            pendingTaskManagerSlotOptional = allocateResource(resourceProfile);
        }

        OptionalConsumer.of(pendingTaskManagerSlotOptional)
                .ifPresent(
                        pendingTaskManagerSlot ->
                                assignPendingTaskManagerSlot(
                                        pendingSlotRequest, pendingTaskManagerSlot))
                .ifNotPresent(
                        () -> {
                            // request can not be fulfilled by any free slot or pending slot that
                            // can be allocated,
                            // check whether it can be fulfilled by allocated slots
                            if (failUnfulfillableRequest
                                    && !isFulfillableByRegisteredOrPendingSlots(
                                            pendingSlotRequest.getResourceProfile())) {
                                throw new UnfulfillableSlotRequestException(
                                        pendingSlotRequest.getAllocationId(),
                                        pendingSlotRequest.getResourceProfile());
                            }
                        });
    }

    private Optional<PendingTaskManagerSlot> findFreeMatchingPendingTaskManagerSlot(
            ResourceProfile requiredResourceProfile) {
        for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if (pendingTaskManagerSlot.getAssignedPendingSlotRequest() == null
                    && pendingTaskManagerSlot
                            .getResourceProfile()
                            .isMatching(requiredResourceProfile)) {
                return Optional.of(pendingTaskManagerSlot);
            }
        }

        return Optional.empty();
    }

    private boolean isFulfillableByRegisteredOrPendingSlots(ResourceProfile resourceProfile) {
        for (TaskManagerSlot slot : slots.values()) {
            if (slot.getResourceProfile().isMatching(resourceProfile)) {
                return true;
            }
        }

        for (PendingTaskManagerSlot slot : pendingSlots.values()) {
            if (slot.getResourceProfile().isMatching(resourceProfile)) {
                return true;
            }
        }

        return false;
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewSlot) {
        return getNumberRegisteredSlots() + getNumberPendingTaskManagerSlots() + numNewSlot
                > maxSlotNum;
    }

    private void allocateRedundantTaskManagers(int number) {
        int allocatedNumber = allocateResources(number);
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
     * @param workerNum the number of workers to allocate.
     * @return the number of allocated workers successfully.
     */
    private int allocateResources(int workerNum) {
        int allocatedWorkerNum = 0;
        for (int i = 0; i < workerNum; ++i) {
            if (allocateResource(defaultSlotResourceProfile).isPresent()) {
                ++allocatedWorkerNum;
            } else {
                break;
            }
        }
        return allocatedWorkerNum;
    }

    private Optional<PendingTaskManagerSlot> allocateResource(
            ResourceProfile requestedSlotResourceProfile) {
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

        if (!resourceActions.allocateResource(defaultWorkerResourceSpec)) {
            // resource cannot be allocated
            return Optional.empty();
        }

        PendingTaskManagerSlot pendingTaskManagerSlot = null;
        for (int i = 0; i < numSlotsPerWorker; ++i) {
            pendingTaskManagerSlot = new PendingTaskManagerSlot(defaultSlotResourceProfile);
            pendingSlots.put(pendingTaskManagerSlot.getTaskManagerSlotId(), pendingTaskManagerSlot);
        }

        return Optional.of(
                Preconditions.checkNotNull(
                        pendingTaskManagerSlot, "At least one pending slot should be created."));
    }

    private void assignPendingTaskManagerSlot(
            PendingSlotRequest pendingSlotRequest, PendingTaskManagerSlot pendingTaskManagerSlot) {
        pendingTaskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
        pendingSlotRequest.assignPendingTaskManagerSlot(pendingTaskManagerSlot);
    }

    /**
     * Allocates the given slot for the given slot request. This entails sending a registration
     * message to the task manager and treating failures.
     *
     * @param taskManagerSlot to allocate for the given slot request
     * @param pendingSlotRequest to allocate the given slot for
     */
    private void allocateSlot(
            TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
        Preconditions.checkState(taskManagerSlot.getState() == SlotState.FREE);

        TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
        TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

        final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
        final AllocationID allocationId = pendingSlotRequest.getAllocationId();
        final SlotID slotId = taskManagerSlot.getSlotId();
        final InstanceID instanceID = taskManagerSlot.getInstanceId();

        taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
        pendingSlotRequest.setRequestFuture(completableFuture);

        returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

        if (taskManagerRegistration == null) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceID + '.');
        }

        taskManagerRegistration.markUsed();

        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        slotId,
                        pendingSlotRequest.getJobId(),
                        allocationId,
                        pendingSlotRequest.getResourceProfile(),
                        pendingSlotRequest.getTargetAddress(),
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

        completableFuture.whenCompleteAsync(
                (Acknowledge acknowledge, Throwable throwable) -> {
                    try {
                        if (acknowledge != null) {
                            updateSlot(slotId, allocationId, pendingSlotRequest.getJobId());
                        } else {
                            if (throwable instanceof SlotOccupiedException) {
                                SlotOccupiedException exception = (SlotOccupiedException) throwable;
                                updateSlot(
                                        slotId, exception.getAllocationId(), exception.getJobId());
                            } else {
                                removeSlotRequestFromSlot(slotId, allocationId);
                            }

                            if (!(throwable instanceof CancellationException)) {
                                handleFailedSlotRequest(slotId, allocationId, throwable);
                            } else {
                                LOG.debug(
                                        "Slot allocation request {} has been cancelled.",
                                        allocationId,
                                        throwable);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error while completing the slot allocation.", e);
                    }
                },
                mainThreadExecutor);
    }

    private void returnPendingTaskManagerSlotIfAssigned(PendingSlotRequest pendingSlotRequest) {
        final PendingTaskManagerSlot pendingTaskManagerSlot =
                pendingSlotRequest.getAssignedPendingTaskManagerSlot();
        if (pendingTaskManagerSlot != null) {
            pendingTaskManagerSlot.unassignPendingSlotRequest();
            pendingSlotRequest.unassignPendingTaskManagerSlot();
        }
    }

    /**
     * Handles a free slot. It first tries to find a pending slot request which can be fulfilled. If
     * there is no such request, then it will add the slot to the set of free slots.
     *
     * @param freeSlot to find a new slot request for
     */
    private void handleFreeSlot(TaskManagerSlot freeSlot) {
        Preconditions.checkState(freeSlot.getState() == SlotState.FREE);

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
     * @param cause for removing the slots
     */
    private void removeSlots(Iterable<SlotID> slotsToRemove, Exception cause) {
        for (SlotID slotId : slotsToRemove) {
            removeSlot(slotId, cause);
        }
    }

    /**
     * Removes the given slot from the slot manager.
     *
     * @param slotId identifying the slot to remove
     * @param cause for removing the slot
     */
    private void removeSlot(SlotID slotId, Exception cause) {
        TaskManagerSlot slot = slots.remove(slotId);

        if (null != slot) {
            freeSlots.remove(slotId);

            if (slot.getState() == SlotState.PENDING) {
                // reject the pending slot request --> triggering a new allocation attempt
                rejectPendingSlotRequest(slot.getAssignedSlotRequest(), cause);
            }

            AllocationID oldAllocationId = slot.getAllocationId();

            if (oldAllocationId != null) {
                fulfilledSlotRequests.remove(oldAllocationId);

                resourceActions.notifyAllocationFailure(slot.getJobId(), oldAllocationId, cause);
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
            if (taskManagerSlot.getState() == SlotState.PENDING
                    && Objects.equals(
                            allocationId,
                            taskManagerSlot.getAssignedSlotRequest().getAllocationId())) {

                TaskManagerRegistration taskManagerRegistration =
                        taskManagerRegistrations.get(taskManagerSlot.getInstanceId());

                if (taskManagerRegistration == null) {
                    throw new IllegalStateException(
                            "Trying to remove slot request from slot for which there is no TaskManager "
                                    + taskManagerSlot.getInstanceId()
                                    + " is registered.");
                }

                // clear the pending slot request
                taskManagerSlot.clearPendingSlotRequest();

                updateSlotState(taskManagerSlot, taskManagerRegistration, null, null);
            } else {
                LOG.debug("Ignore slot request removal for slot {}.", slotId);
            }
        } else {
            LOG.debug(
                    "There was no slot with {} registered. Probably this slot has been already freed.",
                    slotId);
        }
    }

    /**
     * Handles a failed slot request. The slot manager tries to find a new slot fulfilling the
     * resource requirements for the failed slot request.
     *
     * @param slotId identifying the slot which was assigned to the slot request before
     * @param allocationId identifying the failed slot request
     * @param cause of the failure
     */
    private void handleFailedSlotRequest(
            SlotID slotId, AllocationID allocationId, Throwable cause) {
        PendingSlotRequest pendingSlotRequest = pendingSlotRequests.get(allocationId);

        LOG.debug(
                "Slot request with allocation id {} failed for slot {}.",
                allocationId,
                slotId,
                cause);

        if (null != pendingSlotRequest) {
            pendingSlotRequest.setRequestFuture(null);

            try {
                internalRequestSlot(pendingSlotRequest);
            } catch (ResourceManagerException e) {
                pendingSlotRequests.remove(allocationId);

                resourceActions.notifyAllocationFailure(
                        pendingSlotRequest.getJobId(), allocationId, e);
            }
        } else {
            LOG.debug(
                    "There was not pending slot request with allocation id {}. Probably the request has been fulfilled or cancelled.",
                    allocationId);
        }
    }

    /**
     * Rejects the pending slot request by failing the request future with a {@link
     * SlotAllocationException}.
     *
     * @param pendingSlotRequest to reject
     * @param cause of the rejection
     */
    private void rejectPendingSlotRequest(PendingSlotRequest pendingSlotRequest, Exception cause) {
        CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

        if (null != request) {
            request.completeExceptionally(new SlotAllocationException(cause));
        } else {
            LOG.debug(
                    "Cannot reject pending slot request {}, since no request has been sent.",
                    pendingSlotRequest.getAllocationId());
        }
    }

    /**
     * Cancels the given slot request.
     *
     * @param pendingSlotRequest to cancel
     */
    private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
        CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

        returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

        if (null != request) {
            request.cancel(false);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    @VisibleForTesting
    void checkTaskManagerTimeoutsAndRedundancy() {
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
                    // ResourceActions.releaseResource call
                    timedOutTaskManagers.add(taskManagerRegistration);
                }
            }

            int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - freeSlots.size();
            if (freeSlots.size() == slots.size()) {
                // No need to keep redundant taskManagers if no job is running.
                releaseTaskExecutors(timedOutTaskManagers, timedOutTaskManagers.size());
            } else if (slotsDiff > 0) {
                // Keep enough redundant taskManagers from time to time.
                int requiredTaskManagers = MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
                allocateRedundantTaskManagers(requiredTaskManagers);
            } else {
                // second we trigger the release resource callback which can decide upon the
                // resource release
                int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
                releaseTaskExecutors(
                        timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
            }
        }
    }

    private void releaseTaskExecutors(
            ArrayList<TaskManagerRegistration> timedOutTaskManagers, int releaseNum) {
        for (int index = 0; index < releaseNum; ++index) {
            if (waitResultConsumedBeforeRelease) {
                releaseTaskExecutorIfPossible(timedOutTaskManagers.get(index));
            } else {
                releaseTaskExecutor(timedOutTaskManagers.get(index).getInstanceId());
            }
        }
    }

    private void releaseTaskExecutorIfPossible(TaskManagerRegistration taskManagerRegistration) {
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
                                releaseTaskExecutor(timedOutTaskManagerId);
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseTaskExecutor(InstanceID timedOutTaskManagerId) {
        final FlinkException cause = new FlinkException("TaskExecutor exceeded the idle timeout.");
        LOG.debug(
                "Release TaskExecutor {} because it exceeded the idle timeout.",
                timedOutTaskManagerId);
        resourceActions.releaseResource(timedOutTaskManagerId, cause);
    }

    private void checkSlotRequestTimeouts() {
        if (!pendingSlotRequests.isEmpty()) {
            long currentTime = System.currentTimeMillis();

            Iterator<Map.Entry<AllocationID, PendingSlotRequest>> slotRequestIterator =
                    pendingSlotRequests.entrySet().iterator();

            while (slotRequestIterator.hasNext()) {
                PendingSlotRequest slotRequest = slotRequestIterator.next().getValue();

                if (currentTime - slotRequest.getCreationTimestamp()
                        >= slotRequestTimeout.toMilliseconds()) {
                    slotRequestIterator.remove();

                    if (slotRequest.isAssigned()) {
                        cancelPendingSlotRequest(slotRequest);
                    }

                    resourceActions.notifyAllocationFailure(
                            slotRequest.getJobId(),
                            slotRequest.getAllocationId(),
                            new TimeoutException("The allocation could not be fulfilled in time."));
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void internalUnregisterTaskManager(
            TaskManagerRegistration taskManagerRegistration, Exception cause) {
        Preconditions.checkNotNull(taskManagerRegistration);

        removeSlots(taskManagerRegistration.getSlots(), cause);
    }

    private boolean checkDuplicateRequest(AllocationID allocationId) {
        return pendingSlotRequests.containsKey(allocationId)
                || fulfilledSlotRequests.containsKey(allocationId);
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
    PendingSlotRequest getSlotRequest(AllocationID allocationId) {
        return pendingSlotRequests.get(allocationId);
    }

    @VisibleForTesting
    boolean isTaskManagerIdle(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (null != taskManagerRegistration) {
            return taskManagerRegistration.isIdle();
        } else {
            return false;
        }
    }
}
