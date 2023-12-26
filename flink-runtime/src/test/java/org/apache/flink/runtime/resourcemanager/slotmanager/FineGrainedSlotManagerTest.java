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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests of {@link FineGrainedSlotManager}. */
class FineGrainedSlotManagerTest extends FineGrainedSlotManagerTestBase {

    private static final ResourceProfile LARGE_SLOT_RESOURCE_PROFILE =
            DEFAULT_TOTAL_RESOURCE_PROFILE.multiply(2);
    private static final ResourceProfile LARGE_TOTAL_RESOURCE_PROFILE =
            LARGE_SLOT_RESOURCE_PROFILE.multiply(2);

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy(
            SlotManagerConfiguration slotManagerConfiguration) {
        return Optional.empty();
    }

    // ---------------------------------------------------------------------------------------------
    // Initialize and close
    // ---------------------------------------------------------------------------------------------

    @Test
    void testInitializeAndClose() throws Exception {
        new Context() {
            {
                runTest(() -> {});
            }
        };
    }

    @Test
    void testCloseAfterSuspendDoesNotThrowException() throws Exception {
        new Context() {
            {
                runTest(() -> getSlotManager().suspend());
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Register / unregister TaskManager and slot status reconciliation
    // ---------------------------------------------------------------------------------------------

    /** Tests that we can register task manager at the slot manager. */
    @Test
    void testTaskManagerRegistration() throws Exception {
        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture = new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getSlotManager().getNumberRegisteredSlots())
                                    .isEqualTo(DEFAULT_NUM_SLOTS_PER_WORKER);
                            assertThat(getTaskManagerTracker().getRegisteredTaskManagers())
                                    .hasSize(1);
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getRegisteredTaskManager(
                                                            taskManagerConnection.getInstanceID()))
                                    .isPresent();
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getRegisteredTaskManager(
                                                            taskManagerConnection.getInstanceID())
                                                    .get()
                                                    .getAvailableResource())
                                    .isEqualTo(DEFAULT_TOTAL_RESOURCE_PROFILE);
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getRegisteredTaskManager(
                                                            taskManagerConnection.getInstanceID())
                                                    .get()
                                                    .getTotalResource())
                                    .isEqualTo(DEFAULT_TOTAL_RESOURCE_PROFILE);
                        });
            }
        };
    }

    /** Tests that un-registration of task managers will free and remove all allocated slots. */
    @Test
    void testTaskManagerUnregistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final AllocationID allocationId = new AllocationID();
        final SlotReport slotReport =
                new SlotReport(
                        createAllocatedSlotStatus(
                                new JobID(), allocationId, DEFAULT_SLOT_RESOURCE_PROFILE));
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture = new CompletableFuture<>();
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
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getTaskManagerTracker().getRegisteredTaskManagers())
                                    .hasSize(1);
                            final Optional<TaskManagerSlotInformation> slot =
                                    getTaskManagerTracker().getAllocatedOrPendingSlot(allocationId);
                            assertThat(slot).isPresent();
                            assertThat(slot.get().getState()).isSameAs(SlotState.ALLOCATED);

                            runInMainThread(
                                    () ->
                                            unRegisterTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .unregisterTaskManager(
                                                                    taskManagerConnection
                                                                            .getInstanceID(),
                                                                    TEST_EXCEPTION)));

                            assertThat(assertFutureCompleteAndReturn(unRegisterTaskManagerFuture))
                                    .isTrue();
                            assertThat(getTaskManagerTracker().getRegisteredTaskManagers())
                                    .isEmpty();
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getAllocatedOrPendingSlot(allocationId))
                                    .isNotPresent();
                        });
            }
        };
    }

    /** Tests that we can matched task manager will deduct pending task manager. */
    @Test
    void testTaskManagerRegistrationDeductPendingTaskManager() throws Exception {
        final TaskExecutorConnection taskExecutionConnection1 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection2 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutionConnection3 = createTaskExecutorConnection();
        final SlotReport slotReportWithAllocatedSlot =
                new SlotReport(
                        createAllocatedSlotStatus(
                                new JobID(), new AllocationID(), DEFAULT_SLOT_RESOURCE_PROFILE));
        new Context() {
            {
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        (jobRequirements, ignore) -> {
                            assertThat(jobRequirements).hasSize(1);
                            JobID jobID = jobRequirements.keySet().stream().findFirst().get();
                            ResourceAllocationResult.Builder builder =
                                    ResourceAllocationResult.builder();
                            final PendingTaskManager pendingTaskManager;
                            if (getTaskManagerTracker().getPendingTaskManagers().isEmpty()) {
                                pendingTaskManager =
                                        new PendingTaskManager(
                                                DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                DEFAULT_NUM_SLOTS_PER_WORKER);
                            } else {
                                pendingTaskManager =
                                        getTaskManagerTracker()
                                                .getPendingTaskManagers()
                                                .iterator()
                                                .next();
                            }
                            builder.addPendingTaskManagerAllocate(pendingTaskManager);
                            builder.addAllocationOnPendingResource(
                                    jobID,
                                    pendingTaskManager.getPendingTaskManagerId(),
                                    DEFAULT_SLOT_RESOURCE_PROFILE);
                            return builder.build();
                        });

                slotManagerConfigurationBuilder.setRequirementCheckDelay(Duration.ZERO);

                runTest(
                        () -> {
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture1 = new CompletableFuture<>();
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture2 = new CompletableFuture<>();
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture3 = new CompletableFuture<>();
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirementsForSingleSlot());
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

                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture1))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getTaskManagerTracker().getPendingTaskManagers()).hasSize(1);

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

                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture2))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getTaskManagerTracker().getPendingTaskManagers()).hasSize(1);

                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture3.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskExecutionConnection3,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture3))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getTaskManagerTracker().getPendingTaskManagers()).isEmpty();
                        });
            }
        };
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    void testReceivingUnknownSlotReport() throws Exception {
        final InstanceID unknownInstanceID = new InstanceID();
        final SlotReport unknownSlotReport = new SlotReport();
        new Context() {
            {
                runTest(
                        () -> {
                            // check that we don't have any slots registered
                            assertThat(getSlotManager().getNumberRegisteredSlots()).isEqualTo(0);

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
                            assertThat(assertFutureCompleteAndReturn(reportSlotFuture)).isFalse();
                            assertThat(getSlotManager().getNumberRegisteredSlots()).isEqualTo(0);
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Handle result from ResourceAllocationStrategy
    // ---------------------------------------------------------------------------------------------

    @Test
    void testSlotAllocationAccordingToStrategyResult() throws Exception {
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
                            assertThat(requestSlot.f1).isEqualTo(jobId);
                            assertThat(requestSlot.f3).isEqualTo(DEFAULT_SLOT_RESOURCE_PROFILE);
                        });
            }
        };
    }

    @Test
    void testRequestNewResourcesAccordingToStrategyResult() throws Exception {
        final JobID jobId = new JobID();
        final AtomicInteger requestCount = new AtomicInteger(0);
        final List<CompletableFuture<Void>> allocateResourceFutures = new ArrayList<>();
        allocateResourceFutures.add(new CompletableFuture<>());
        allocateResourceFutures.add(new CompletableFuture<>());
        new Context() {
            {
                PendingTaskManager pendingTaskManager =
                        new PendingTaskManager(
                                DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            assertThat(requestCount.get()).isLessThan(2);
                            if (!resourceDeclarations.isEmpty()) {
                                allocateResourceFutures
                                        .get(requestCount.getAndIncrement())
                                        .complete(null);
                            }
                        });
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        ((jobIDCollectionMap, taskManagerResourceInfoProvider) ->
                                ResourceAllocationResult.builder()
                                        .addPendingTaskManagerAllocate(pendingTaskManager)
                                        .addAllocationOnPendingResource(
                                                jobId,
                                                pendingTaskManager.getPendingTaskManagerId(),
                                                DEFAULT_SLOT_RESOURCE_PROFILE)
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
                            assertThat(requestCount.get()).isEqualTo(1);
                        });
            }
        };
    }

    @Test
    void testSlotAllocationForPendingTaskManagerWillBeRespected() throws Exception {
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
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            if (!resourceDeclarations.isEmpty()) {
                                requestResourceFuture.complete(null);
                            }
                        });
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
                            assertThat(requestSlot.f1).isEqualTo(jobId);
                            assertThat(requestSlot.f3).isEqualTo(DEFAULT_SLOT_RESOURCE_PROFILE);
                        });
            }
        };
    }

    @Test
    void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
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
                resourceEventListenerBuilder.setNotEnoughResourceAvailableConsumer(
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
                                assertThat(notEnoughResourceNotifications).isEmpty();

                                // re-enable notifications which should also trigger another
                                // resource check
                                runInMainThread(
                                        () -> getSlotManager().setFailUnfulfillableRequest(true));
                            }

                            assertFutureCompleteAndReturn(notifyNotEnoughResourceFuture);
                            assertThat(notEnoughResourceNotifications).hasSize(1);
                            final Tuple2<JobID, Collection<ResourceRequirement>> notification =
                                    notEnoughResourceNotifications.get(0);
                            assertThat(notification.f0).isEqualTo(jobId);
                        });
            }
        };
    }

    /**
     * Test that checkResourceRequirements will only be triggered once after multiple trigger
     * function calls.
     */
    @Test
    void testRequirementCheckOnlyTriggeredOnce() throws Exception {
        new Context() {
            {
                final List<CompletableFuture<Void>> checkRequirementFutures = new ArrayList<>();
                checkRequirementFutures.add(new CompletableFuture<>());
                checkRequirementFutures.add(new CompletableFuture<>());
                final Duration requirementCheckDelay = Duration.ofMillis(50);
                resourceAllocationStrategyBuilder.setTryFulfillRequirementsFunction(
                        (ignored1, ignored2) -> {
                            if (checkRequirementFutures.get(0).isDone()) {
                                checkRequirementFutures.get(1).complete(null);
                            } else {
                                checkRequirementFutures.get(0).complete(null);
                            }
                            return ResourceAllocationResult.builder().build();
                        });
                slotManagerConfigurationBuilder.setRequirementCheckDelay(requirementCheckDelay);
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
                            final CompletableFuture<Void> registrationFuture =
                                    new CompletableFuture<>();
                            final long start = System.nanoTime();
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
                                        registrationFuture.complete(null);
                                    });

                            assertFutureCompleteAndReturn(registrationFuture);
                            final long registrationTime = (System.nanoTime() - start) / 1_000_000;
                            assumeThat(registrationTime < requirementCheckDelay.toMillis())
                                    .as(
                                            "The time of process requirement and register task manager must not take longer than the requirement check delay. If it does, then this indicates a very slow machine.")
                                    .isTrue();

                            assertFutureCompleteAndReturn(checkRequirementFutures.get(0));
                            assertFutureNotComplete(checkRequirementFutures.get(1));

                            // checkTimes will not increase when there's no events
                            Thread.sleep(requirementCheckDelay.toMillis() * 2);
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

    @Test
    void testMaxTotalResourceCpuExceeded() throws Exception {
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
    void testGetResourceOverview() throws Exception {
        final TaskExecutorConnection taskExecutorConnection1 = createTaskExecutorConnection();
        final TaskExecutorConnection taskExecutorConnection2 = createTaskExecutorConnection();
        final ResourceID resourceId1 = ResourceID.generate();
        final ResourceID resourceId2 = ResourceID.generate();

        final SlotID slotId1 = new SlotID(resourceId1, 0);
        final SlotID slotId2 = new SlotID(resourceId2, 0);
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        final SlotStatus slotStatus1 =
                new SlotStatus(slotId1, resourceProfile1, new JobID(), new AllocationID());
        final SlotStatus slotStatus2 =
                new SlotStatus(slotId2, resourceProfile2, new JobID(), new AllocationID());
        final SlotReport slotReport1 = new SlotReport(slotStatus1);
        final SlotReport slotReport2 = new SlotReport(slotStatus2);

        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture1 = new CompletableFuture<>();
                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture2 = new CompletableFuture<>();
                            runInMainThread(
                                    () -> {
                                        registerTaskManagerFuture1.complete(
                                                getSlotManager()
                                                        .registerTaskManager(
                                                                taskExecutorConnection1,
                                                                slotReport1,
                                                                resourceProfile1.multiply(2),
                                                                resourceProfile1));
                                        registerTaskManagerFuture2.complete(
                                                getSlotManager()
                                                        .registerTaskManager(
                                                                taskExecutorConnection2,
                                                                slotReport2,
                                                                resourceProfile2.multiply(2),
                                                                resourceProfile2));
                                    });
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture1))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture2))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getSlotManager().getFreeResource())
                                    .isEqualTo(resourceProfile1.merge(resourceProfile2));
                            assertThat(
                                            getSlotManager()
                                                    .getFreeResourceOf(
                                                            taskExecutorConnection1
                                                                    .getInstanceID()))
                                    .isEqualTo(resourceProfile1);
                            assertThat(
                                            getSlotManager()
                                                    .getFreeResourceOf(
                                                            taskExecutorConnection2
                                                                    .getInstanceID()))
                                    .isEqualTo(resourceProfile2);
                            assertThat(getSlotManager().getRegisteredResource())
                                    .isEqualTo(
                                            resourceProfile1.merge(resourceProfile2).multiply(2));
                            assertThat(
                                            getSlotManager()
                                                    .getRegisteredResourceOf(
                                                            taskExecutorConnection1
                                                                    .getInstanceID()))
                                    .isEqualTo(resourceProfile1.multiply(2));
                            assertThat(
                                            getSlotManager()
                                                    .getRegisteredResourceOf(
                                                            taskExecutorConnection2
                                                                    .getInstanceID()))
                                    .isEqualTo(resourceProfile2.multiply(2));
                        });
            }
        };
    }

    @Test
    void testMaxTotalResourceMemoryExceeded() throws Exception {
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
        final AtomicInteger requestCount = new AtomicInteger(0);
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

                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            if (!resourceDeclarations.isEmpty()) {
                                assertThat(requestCount.get()).isLessThan(2);
                                allocateResourceFutures
                                        .get(requestCount.getAndIncrement())
                                        .complete(null);
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
        final CompletableFuture<SlotManager.RegistrationResult> registerTaskManagerFuture1 =
                new CompletableFuture<>();
        final CompletableFuture<SlotManager.RegistrationResult> registerTaskManagerFuture2 =
                new CompletableFuture<>();

        new Context() {
            {
                maxTotalResourceSetter.accept(slotManagerConfigurationBuilder);

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
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture1))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getTaskManagerTracker().getRegisteredTaskManagers())
                                    .hasSize(1);
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getRegisteredTaskManager(
                                                            taskManagerConnection1.getInstanceID()))
                                    .isPresent();

                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture2.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskManagerConnection2,
                                                                    new SlotReport(),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture2))
                                    .isEqualTo(SlotManager.RegistrationResult.REJECTED);
                            assertThat(getTaskManagerTracker().getRegisteredTaskManagers())
                                    .hasSize(1);
                            assertThat(
                                            getTaskManagerTracker()
                                                    .getRegisteredTaskManager(
                                                            taskManagerConnection2.getInstanceID()))
                                    .isNotPresent();
                        });
            }
        };
    }

    @Test
    void testMetricsUnregisteredWhenSuspending() throws Exception {
        testAccessMetricValueDuringItsUnregister(SlotManager::suspend);
    }

    @Test
    void testMetricsUnregisteredWhenClosing() throws Exception {
        testAccessMetricValueDuringItsUnregister(AutoCloseable::close);
    }

    private void testAccessMetricValueDuringItsUnregister(
            ThrowingConsumer<SlotManager, Exception> closeFn) throws Exception {
        final AtomicInteger registeredMetrics = new AtomicInteger();
        final MetricRegistry metricRegistry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer((a, b, c) -> registeredMetrics.incrementAndGet())
                        .setUnregisterConsumer((a, b, c) -> registeredMetrics.decrementAndGet())
                        .build();

        final Context context = new Context();
        context.setSlotManagerMetricGroup(
                SlotManagerMetricGroup.create(metricRegistry, "localhost"));

        context.runTest(
                () -> {
                    // sanity check to ensure metrics were actually registered
                    assertThat(registeredMetrics.get()).isGreaterThan(0);
                    context.runInMainThreadAndWait(
                            () -> {
                                assertThatNoException()
                                        .isThrownBy(() -> closeFn.accept(context.getSlotManager()));
                            });
                    assertThat(registeredMetrics.get()).isEqualTo(0);
                });
    }

    @Test
    void testReclaimInactiveSlotsOnClearRequirements() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final CompletableFuture<JobID> freeInactiveSlotsJobIdFuture =
                                    new CompletableFuture<>();

                            final JobID jobId = new JobID();
                            final TestingTaskExecutorGateway taskExecutorGateway =
                                    new TestingTaskExecutorGatewayBuilder()
                                            .setFreeInactiveSlotsConsumer(
                                                    freeInactiveSlotsJobIdFuture::complete)
                                            .createTestingTaskExecutorGateway();
                            final TaskExecutorConnection taskExecutionConnection =
                                    createTaskExecutorConnection(taskExecutorGateway);

                            final CompletableFuture<SlotManager.RegistrationResult>
                                    registerTaskManagerFuture = new CompletableFuture<>();
                            runInMainThread(
                                    () ->
                                            registerTaskManagerFuture.complete(
                                                    getSlotManager()
                                                            .registerTaskManager(
                                                                    taskExecutionConnection,
                                                                    new SlotReport(
                                                                            createAllocatedSlotStatus(
                                                                                    jobId,
                                                                                    new AllocationID(),
                                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);

                            // setup initial requirements, which should not trigger slots being
                            // reclaimed
                            runInMainThreadAndWait(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(jobId, 2)));
                            assertFutureNotComplete(freeInactiveSlotsJobIdFuture);

                            // set requirements to 0, which should not trigger slots being reclaimed
                            runInMainThreadAndWait(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            ResourceRequirements.empty(
                                                                    jobId, "foobar")));
                            assertFutureNotComplete(freeInactiveSlotsJobIdFuture);

                            // clear requirements, which should trigger slots being reclaimed
                            runInMainThreadAndWait(
                                    () -> getSlotManager().clearResourceRequirements(jobId));
                            assertThat(freeInactiveSlotsJobIdFuture.get()).isEqualTo(jobId);
                        });
            }
        };
    }

    @Test
    void testClearResourceRequirementsWithPendingTaskManager() throws Exception {
        new Context() {
            {
                final JobID jobId = new JobID();
                final CompletableFuture<Void> allocateResourceFuture = new CompletableFuture<>();

                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> allocateResourceFuture.complete(null));

                final PendingTaskManager pendingTaskManager1 =
                        new PendingTaskManager(
                                DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
                final PendingTaskManager pendingTaskManager2 =
                        new PendingTaskManager(
                                DEFAULT_TOTAL_RESOURCE_PROFILE, DEFAULT_NUM_SLOTS_PER_WORKER);
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
                            // assign allocations to pending task managers
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(jobId, 2)));
                            assertFutureCompleteAndReturn(allocateResourceFuture);

                            // cancel all slot requests, will trigger
                            // PendingTaskManager#clearPendingAllocationsOfJob
                            runInMainThreadAndWait(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            ResourceRequirements.empty(
                                                                    jobId, "foobar")));

                            // disconnect to job master,will trigger
                            // PendingTaskManager#clearPendingAllocationsOfJob again
                            CompletableFuture<Void> clearFuture = new CompletableFuture<>();
                            runInMainThread(
                                    () -> {
                                        try {
                                            getSlotManager().clearResourceRequirements(jobId);
                                        } catch (Exception e) {
                                            clearFuture.completeExceptionally(e);
                                        }
                                        clearFuture.complete(null);
                                    });

                            assertFutureCompleteAndReturn(clearFuture);
                        });
            }
        };
    }
}
