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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests of {@link FineGrainedSlotManager}. */
public class FineGrainedSlotManagerTest extends FineGrainedSlotManagerTestBase {

    private static final ResourceProfile LARGE_SLOT_RESOURCE_PROFILE =
            DEFAULT_TOTAL_RESOURCE_PROFILE.multiply(2);
    private static final ResourceProfile LARGE_TOTAL_RESOURCE_PROFILE =
            LARGE_SLOT_RESOURCE_PROFILE.multiply(2);

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy() {
        return Optional.empty();
    }

    // ---------------------------------------------------------------------------------------------
    // Initialize and close
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testInitializeAndClose() throws Exception {
        new Context() {
            {
                runTest(() -> {});
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Register / unregister TaskManager and and slot status reconciliation
    // ---------------------------------------------------------------------------------------------

    /** Tests that we can register task manager at the slot manager. */
    @Test
    public void testTaskManagerRegistration() throws Exception {
        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<Boolean> registerTaskManagerFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture),
                                    is(true));
                            assertThat(
                                    getSlotManager().getNumberRegisteredSlots(),
                                    equalTo(DEFAULT_NUM_SLOTS_PER_WORKER));
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    equalTo(1));
                            assertTrue(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .isPresent());
                            assertThat(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .get()
                                            .getAvailableResource(),
                                    equalTo(DEFAULT_TOTAL_RESOURCE_PROFILE));
                            assertThat(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection.getInstanceID())
                                            .get()
                                            .getTotalResource(),
                                    equalTo(DEFAULT_TOTAL_RESOURCE_PROFILE));
                        });
            }
        };
    }

    /** Tests that un-registration of task managers will free and remove all allocated slots. */
    @Test
    public void testTaskManagerUnregistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final AllocationID allocationId = new AllocationID();
        final SlotReport slotReport =
                new SlotReport(
                        createAllocatedSlotStatus(allocationId, DEFAULT_SLOT_RESOURCE_PROFILE));
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<Boolean> registerTaskManagerFuture =
                                    new CompletableFuture<>();
                            final CompletableFuture<Boolean> unRegisterTaskManagerFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection,
                                                                    slotReport,
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture),
                                    is(true));
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    is(1));
                            final Optional<TaskManagerSlotInformation> slot =
                                    getTaskManagerTracker().getAllocatedOrPendingSlot(allocationId);
                            assertTrue(slot.isPresent());
                            assertTrue(slot.get().getState() == SlotState.ALLOCATED);

                            runInMainThread(
                                    () ->
                                            unRegisterTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .unregisterTaskManager(
                                                                    taskManagerConnection
                                                                            .getInstanceID(),
                                                                    TEST_EXCEPTION)));

                            assertThat(
                                    assertFutureCompleteAndReturn(unRegisterTaskManagerFuture),
                                    is(true));
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers(),
                                    is(empty()));
                            assertFalse(
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(allocationId)
                                            .isPresent());
                        });
            }
        };
    }

    /** Tests that we can matched task manager will deduct pending task manager. */
    @Test
    public void testTaskManagerRegistrationDeductPendingTaskManager() throws Exception {
        final TaskExecutorConnection taskExecutionConnection1 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection2 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection3 = createTaskExecutorConnection();
        final SlotReport slotReportWithAllocatedSlot =
                new SlotReport(
                        createAllocatedSlotStatus(
                                new AllocationID(), DEFAULT_SLOT_RESOURCE_PROFILE));
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<Boolean> registerTaskManagerFuture1 =
                                    new CompletableFuture<>();
                            final CompletableFuture<Boolean> registerTaskManagerFuture2 =
                                    new CompletableFuture<>();
                            final CompletableFuture<Boolean> registerTaskManagerFuture3 =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () -> {
                                        getTaskManagerTracker()
                                                .addPendingTaskManager(
                                                        new PendingTaskManager(
                                                                DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                DEFAULT_NUM_SLOTS_PER_WORKER));
                                        // task manager with allocated slot cannot deduct pending
                                        // task manager
                                        registerTaskManagerFuture1.complete(
                                                getSlotManager()
                                                        .registerTaskManager(
                                                                taskExecutionConnection1,
                                                                slotReportWithAllocatedSlot,
                                                                DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                DEFAULT_SLOT_RESOURCE_PROFILE));
                                    });

                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture1),
                                    is(true));
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(1));

                            // task manager with mismatched resource cannot deduct
                            // pending task manager
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture2.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskExecutionConnection2,
                                                                    new SlotReport(),
                                                                    LARGE_TOTAL_RESOURCE_PROFILE,
                                                                    LARGE_SLOT_RESOURCE_PROFILE)));

                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture2),
                                    is(true));
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(1));

                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture3.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskExecutionConnection3,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture3),
                                    is(true));
                            assertThat(
                                    getTaskManagerTracker().getPendingTaskManagers().size(), is(0));
                        });
            }
        };
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    public void testReceivingUnknownSlotReport() throws Exception {
        final InstanceID unknownInstanceID = new InstanceID();
        final SlotReport unknownSlotReport = new SlotReport();
        new Context() {
            {
                runTest(
                        () -> {
                            // check that we don't have any slots registered
                            assertThat(getSlotManager().getNumberRegisteredSlots(), is(0));

                            // this should not update anything since the instance id is not known to
                            // the slot manager
                            final CompletableFuture<Boolean> reportSlotFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            reportSlotFuture.complete(
                                                    getSlotManager()
                                                            .reportSlotStatus(
                                                                    unknownInstanceID,
                                                                    unknownSlotReport)));
                            assertFalse(assertFutureCompleteAndReturn(reportSlotFuture));
                            assertThat(getSlotManager().getNumberRegisteredSlots(), is(0));
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Handle result from ResourceAllocationStrategy
    // ---------------------------------------------------------------------------------------------

    @Test
    public void testSlotAllocationAccordingToStrategyResult() throws Exception {
        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestSlotFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestSlotFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final JobID jobId = new JobID();
        final SlotReport slotReport = new SlotReport();
        new Context() {
            {
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addAllocationOnRegisteredResource(
                                                jobId,
                                                taskManagerConnection.getInstanceID(),
                                                DEFAULT_SLOT_RESOURCE_PROFILE)
                                        .build()));
                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskManagerConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                    });
                            final Tuple6<
                                            SlotID,
                                            JobID,
                                            AllocationID,
                                            ResourceProfile,
                                            String,
                                            ResourceManagerId>
                                    requestSlot = assertFutureCompleteAndReturn(requestSlotFuture);
                            assertEquals(jobId, requestSlot.f1);
                            assertEquals(DEFAULT_SLOT_RESOURCE_PROFILE, requestSlot.f3);
                        });
            }
        };
    }

    @Test
    public void testRequestNewResourcesAccordingToStrategyResult() throws Exception {
        final JobID jobId = new JobID();
        final List<CompletableFuture<Void>> allocateResourceFutures = new ArrayList<>();
        allocateResourceFutures.add(new CompletableFuture<>());
        allocateResourceFutures.add(new CompletableFuture<>());
        new Context() {
            {
                resourceActionsBuilder.setAllocateResourceConsumer(
                        ignored -> {
                            if (allocateResourceFutures.get(0).isDone()) {
                                allocateResourceFutures.get(1).complete(null);
                            } else {
                                allocateResourceFutures.get(0).complete(null);
                            }
                        });
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(
                                                new PendingTaskManager(
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_NUM_SLOTS_PER_WORKER))
                                        .build()));
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(jobId, 1)));
                            assertFutureCompleteAndReturn(allocateResourceFutures.get(0));
                            assertFutureNotComplete(allocateResourceFutures.get(1));
                        });
            }
        };
    }

    @Test
    public void testSlotAllocationForPendingTaskManagerWillBeRespected() throws Exception {
        final JobID jobId = new JobID();
        final CompletableFuture<Void> requestResourceFuture = new CompletableFuture<>();
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(
                        DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestSlotFuture = new CompletableFuture<>();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestSlotFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        new Context() {
            {
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(pendingTaskManager)
                                        .addAllocationOnPendingResource(
                                                jobId,
                                                pendingTaskManager.getPendingTaskManagerId(),
                                                DEFAULT_SLOT_RESOURCE_PROFILE)
                                        .build()));
                resourceActionsBuilder.setAllocateResourceConsumer(
                        ignored -> requestResourceFuture.complete(null));
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(jobId, 1)));
                            assertFutureCompleteAndReturn(requestResourceFuture);
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .registerTaskManager(
                                                            taskManagerConnection,
                                                            new SlotReport(),
                                                            DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                            DEFAULT_SLOT_RESOURCE_PROFILE));
                            final Tuple6<
                                            SlotID,
                                            JobID,
                                            AllocationID,
                                            ResourceProfile,
                                            String,
                                            ResourceManagerId>
                                    requestSlot = assertFutureCompleteAndReturn(requestSlotFuture);
                            assertEquals(jobId, requestSlot.f1);
                            assertEquals(DEFAULT_SLOT_RESOURCE_PROFILE, requestSlot.f3);
                        });
            }
        };
    }

    @Test
    public void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    public void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(true);
    }

    private void testNotificationAboutNotEnoughResources(boolean withNotificationGracePeriod)
            throws Exception {
        final JobID jobId = new JobID();
        final List<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotifications =
                new ArrayList<>();
        final CompletableFuture<Void> notifyNotEnoughResourceFuture = new CompletableFuture<>();
        new Context() {
            {
                resourceActionsBuilder.setNotEnoughResourcesConsumer(
                        (jobId1, acquiredResources) -> {
                            notEnoughResourceNotifications.add(
                                    Tuple2.of(jobId1, acquiredResources));
                            notifyNotEnoughResourceFuture.complete(null);
                        });
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addUnfulfillableJob(jobId)
                                        .build()));

                runTest(
                        () -> {
                            if (withNotificationGracePeriod) {
                                // this should disable notifications
                                runInMainThread(
                                        () -> getSlotManager().setFailUnfulfillableRequest(false));
                            }

                            final ResourceRequirements resourceRequirements =
                                    createResourceRequirements(jobId, 1);
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));

                            if (withNotificationGracePeriod) {
                                assertFutureNotComplete(notifyNotEnoughResourceFuture);
                                assertThat(notEnoughResourceNotifications, empty());

                                // re-enable notifications which should also trigger another
                                // resource check
                                runInMainThread(
                                        () -> getSlotManager().setFailUnfulfillableRequest(true));
                            }

                            assertFutureCompleteAndReturn(notifyNotEnoughResourceFuture);
                            assertThat(notEnoughResourceNotifications, hasSize(1));
                            final Tuple2<JobID, Collection<ResourceRequirement>> notification =
                                    notEnoughResourceNotifications.get(0);
                            assertThat(notification.f0, is(jobId));
                        });
            }
        };
    }

    /**
     * Test that checkResourceRequirements will only be triggered once after multiple trigger
     * function calls.
     */
    @Test
    public void testRequirementCheckOnlyTriggeredOnce() throws Exception {
        new Context() {
            {
                final List<CompletableFuture<Void>> checkRequirementFutures = new ArrayList<>();
                checkRequirementFutures.add(new CompletableFuture<>());
                checkRequirementFutures.add(new CompletableFuture<>());
                final long requirementCheckDelay = 20;
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        (ignored1, ignored2) -> {
                            if (checkRequirementFutures.get(0).isDone()) {
                                checkRequirementFutures.get(1).complete(null);
                            } else {
                                checkRequirementFutures.get(0).complete(null);
                            }
                            return ResourceAllocationResult.builder().build();
                        });
                setRequirementCheckDelay(requirementCheckDelay);
                runTest(
                        () -> {
                            final ResourceRequirements resourceRequirements1 =
                                    createResourceRequirementsForSingleSlot();
                            final ResourceRequirements resourceRequirements2 =
                                    createResourceRequirementsForSingleSlot();
                            final ResourceRequirements resourceRequirements3 =
                                    createResourceRequirementsForSingleSlot();
                            final TaskExecutorConnection taskExecutionConnection =
                                    createTaskExecutorConnection();
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(resourceRequirements1);
                                        getSlotManager()
                                                .processResourceRequirements(resourceRequirements2);
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutionConnection,
                                                        new SlotReport(),
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                    });
                            assertFutureCompleteAndReturn(checkRequirementFutures.get(0));
                            assertFutureNotComplete(checkRequirementFutures.get(1));

                            // checkTimes will not increase when there's no events
                            Thread.sleep(requirementCheckDelay * 2);
                            assertFutureNotComplete(checkRequirementFutures.get(1));

                            // checkTimes will increase again if there's another
                            // processResourceRequirements
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements3));
                            assertFutureCompleteAndReturn(checkRequirementFutures.get(1));
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Task manager timeout
    // ---------------------------------------------------------------------------------------------

    /**
     * Tests that formerly used task managers can timeout after all of their slots have been freed.
     */
    @Test
    public void testTimeoutForUnusedTaskManager() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(50L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final AllocationID allocationId = new AllocationID();
        final TaskExecutorConnection taskExecutionConnection = createTaskExecutorConnection();
        final InstanceID instanceId = taskExecutionConnection.getInstanceID();
        new Context() {
            {
                resourceActionsBuilder.setReleaseResourceConsumer(
                        (instanceID, e) -> releaseResourceFuture.complete(instanceID));
                slotManagerConfigurationBuilder.setTaskManagerTimeout(taskManagerTimeout);
                runTest(
                        () -> {
                            final CompletableFuture<Boolean> registerTaskManagerFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskExecutionConnection,
                                                                    new SlotReport(
                                                                            createAllocatedSlotStatus(
                                                                                    allocationId,
                                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture),
                                    is(true));
                            assertEquals(
                                    getSlotManager().getTaskManagerIdleSince(instanceId),
                                    Long.MAX_VALUE);

                            final CompletableFuture<Long> idleSinceFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .freeSlot(
                                                        new SlotID(
                                                                taskExecutionConnection
                                                                        .getResourceID(),
                                                                0),
                                                        allocationId);
                                        idleSinceFuture.complete(
                                                getSlotManager()
                                                        .getTaskManagerIdleSince(instanceId));
                                    });

                            assertThat(
                                    assertFutureCompleteAndReturn(idleSinceFuture),
                                    not(equalTo(Long.MAX_VALUE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(releaseResourceFuture),
                                    is(equalTo(instanceId)));
                            // A task manager timeout does not remove the slots from the
                            // SlotManager. The receiver of the callback can then decide what to do
                            // with the TaskManager.
                            assertEquals(
                                    DEFAULT_NUM_SLOTS_PER_WORKER,
                                    getSlotManager().getNumberRegisteredSlots());

                            final CompletableFuture<Boolean> unregisterTaskManagerFuture =
                                    new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            unregisterTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .unregisterTaskManager(
                                                                    taskExecutionConnection
                                                                            .getInstanceID(),
                                                                    TEST_EXCEPTION)));
                            assertThat(
                                    assertFutureCompleteAndReturn(unregisterTaskManagerFuture),
                                    is(true));
                            assertEquals(0, getSlotManager().getNumberRegisteredSlots());
                        });
            }
        };
    }

    @Test
    public void testMaxTotalResourceCpuExceeded() throws Exception {
        Consumer<SlotManagerConfigurationBuilder> maxTotalResourceSetter =
                (smConfigBuilder) ->
                        smConfigBuilder.setMaxTotalCpu(
                                DEFAULT_TOTAL_RESOURCE_PROFILE
                                        .getCpuCores()
                                        .multiply(BigDecimal.valueOf(1.5)));

        testMaxTotalResourceExceededAllocateResource(maxTotalResourceSetter);
        testMaxTotalResourceExceededRegisterResource(maxTotalResourceSetter);
    }

    @Test
    public void testMaxTotalResourceMemoryExceeded() throws Exception {
        Consumer<SlotManagerConfigurationBuilder> maxTotalResourceSetter =
                (smConfigBuilder) ->
                        smConfigBuilder.setMaxTotalMem(
                                DEFAULT_TOTAL_RESOURCE_PROFILE.getTotalMemory().multiply(1.5));

        testMaxTotalResourceExceededAllocateResource(maxTotalResourceSetter);
        testMaxTotalResourceExceededRegisterResource(maxTotalResourceSetter);
    }

    private void testMaxTotalResourceExceededAllocateResource(
            Consumer<SlotManagerConfigurationBuilder> maxTotalResourceSetter) throws Exception {
        final JobID jobId = new JobID();
        final List<CompletableFuture<Void>> allocateResourceFutures = new ArrayList<>();
        allocateResourceFutures.add(new CompletableFuture<>());
        allocateResourceFutures.add(new CompletableFuture<>());
        final PendingTaskManager pendingTaskManager1 =
                new PendingTaskManager(
                        DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
        final PendingTaskManager pendingTaskManager2 =
                new PendingTaskManager(
                        DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
        new Context() {
            {
                maxTotalResourceSetter.accept(slotManagerConfigurationBuilder);

                resourceActionsBuilder.setAllocateResourceConsumer(
                        ignored -> {
                            if (allocateResourceFutures.get(0).isDone()) {
                                allocateResourceFutures.get(1).complete(null);
                            } else {
                                allocateResourceFutures.get(0).complete(null);
                            }
                        });

                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(pendingTaskManager1)
                                        .addPendingTaskManagerAllocate(pendingTaskManager2)
                                        .addAllocationOnPendingResource(
                                                jobId,
                                                pendingTaskManager1.getPendingTaskManagerId(),
                                                DEFAULT_SLOT_RESOURCE_PROFILE)
                                        .addAllocationOnPendingResource(
                                                jobId,
                                                pendingTaskManager2.getPendingTaskManagerId(),
                                                DEFAULT_SLOT_RESOURCE_PROFILE)
                                        .build()));
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(jobId, 2)));
                            assertFutureCompleteAndReturn(allocateResourceFutures.get(0));
                            assertFutureNotComplete(allocateResourceFutures.get(1));
                        });
            }
        };
    }

    private void testMaxTotalResourceExceededRegisterResource(
            Consumer<SlotManagerConfigurationBuilder> maxTotalResourceSetter) throws Exception {
        final TaskExecutorConnection taskManagerConnection1 = createTaskExecutorConnection();
        final TaskExecutorConnection taskManagerConnection2 = createTaskExecutorConnection();
        final CompletableFuture<Boolean> registerTaskManagerFuture1 = new CompletableFuture<>();
        final CompletableFuture<Boolean> registerTaskManagerFuture2 = new CompletableFuture<>();
        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();

        new Context() {
            {
                maxTotalResourceSetter.accept(slotManagerConfigurationBuilder);

                resourceActionsBuilder.setReleaseResourceConsumer(
                        (instanceId, ignore) -> releaseResourceFuture.complete(instanceId));

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture1.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection1,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture1),
                                    is(true));
                            assertFutureNotComplete(releaseResourceFuture);
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    equalTo(1));
                            assertTrue(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection1.getInstanceID())
                                            .isPresent());

                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture2.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection2,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(
                                    assertFutureCompleteAndReturn(registerTaskManagerFuture2),
                                    is(false));
                            assertThat(
                                    releaseResourceFuture.get(),
                                    is(taskManagerConnection2.getInstanceID()));
                            assertThat(
                                    getTaskManagerTracker().getRegisteredTaskManagers().size(),
                                    equalTo(1));
                            assertFalse(
                                    getTaskManagerTracker()
                                            .getRegisteredTaskManager(
                                                    taskManagerConnection2.getInstanceID())
                                            .isPresent());
                        });
            }
        };
    }
}
