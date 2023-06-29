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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DeclarativeSlotManager}. */
class DeclarativeSlotManagerTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");

    private static final WorkerResourceSpec WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(100.0)
                    .setTaskHeapMemoryMB(10000)
                    .setTaskOffHeapMemoryMB(10000)
                    .setNetworkMemoryMB(10000)
                    .setManagedMemoryMB(10000)
                    .build();

    @Test
    void testCloseAfterSuspendDoesNotThrowException() throws Exception {
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder().buildAndStartWithDirectExec()) {
            slotManager.suspend();
        }
    }

    /** Tests that we can register task manager and their slots at the slot manager. */
    @Test
    void testTaskManagerRegistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotReport slotReport =
                new SlotReport(
                        Arrays.asList(
                                createFreeSlotStatus(slotId1), createFreeSlotStatus(slotId2)));

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots())
                    .as("The number registered slots does not equal the expected number.")
                    .isEqualTo(2);

            assertThat(slotTracker.getSlot(slotId1)).isNotNull();
            assertThat(slotTracker.getSlot(slotId2)).isNotNull();
        }
    }

    /** Tests that un-registration of task managers will free and remove all registered slots. */
    @Test
    void testTaskManagerUnregistration() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                createTaskExecutorConnection(taskExecutorGateway);
        final ResourceID resourceId = taskManagerConnection.getResourceID();

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotReport slotReport =
                new SlotReport(
                        Arrays.asList(
                                createAllocatedSlotStatus(slotId1), createFreeSlotStatus(slotId2)));

        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots())
                    .as("The number registered slots does not equal the expected number.")
                    .isEqualTo(2);

            slotManager.processResourceRequirements(resourceRequirements);

            slotManager.unregisterTaskManager(
                    taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(0);
        }
    }

    /** Tests that a slot request with no free slots will trigger the resource allocation. */
    @Test
    void testRequirementDeclarationWithoutFreeSlotsTriggersWorkerAllocation() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        CompletableFuture<Collection<ResourceDeclaration>> declareResourceFuture =
                new CompletableFuture<>();
        ResourceAllocator resourceAllocator =
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(declareResourceFuture::complete)
                        .build();

        try (SlotManager slotManager = createSlotManager(resourceManagerId, resourceAllocator)) {

            slotManager.processResourceRequirements(resourceRequirements);

            declareResourceFuture.get();
        }
    }

    /**
     * Tests that blocked slots cannot be used to fulfill requirements, will trigger the new
     * resource allocation.
     */
    @Test
    void testRequirementDeclarationWithBlockedSlotsTriggersWorkerAllocation() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        CompletableFuture<Collection<ResourceDeclaration>> declareResourceFuture =
                new CompletableFuture<>();
        ResourceAllocator resourceAllocator =
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(declareResourceFuture::complete)
                        .build();

        final ResourceID blockedTaskManager = ResourceID.generate();

        try (SlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .buildAndStart(
                                resourceManagerId,
                                Executors.directExecutor(),
                                resourceAllocator,
                                new TestingResourceEventListenerBuilder().build(),
                                blockedTaskManager::equals)) {

            final TaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            final TaskExecutorConnection taskManagerConnection =
                    new TaskExecutorConnection(blockedTaskManager, taskExecutorGateway);

            final SlotID slotId = new SlotID(blockedTaskManager, 0);
            final SlotReport slotReport = new SlotReport(createFreeSlotStatus(slotId));

            // register blocked TM
            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(resourceRequirements);

            declareResourceFuture.get();
        }
    }

    /**
     * Tests that resources continue to be considered missing if we cannot allocate more resources.
     */
    @Test
    void testRequirementDeclarationWithResourceAllocationFailure() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final ResourceAllocator resourceAllocator = NonSupportedResourceAllocatorImpl.INSTANCE;

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceAllocator)) {

            slotManager.processResourceRequirements(resourceRequirements);

            final JobID jobId = resourceRequirements.getJobId();
            assertThat(getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)))
                    .isEqualTo(1);
        }
    }

    /** Tests that resource requirements can be fulfilled with slots that are currently free. */
    @Test
    void testRequirementDeclarationWithFreeSlot() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION);
    }

    /**
     * Tests that resource requirements can be fulfilled with slots that are registered after the
     * requirement declaration.
     */
    @Test
    void testRequirementDeclarationWithPendingSlot() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION);
    }

    private enum RequirementDeclarationScenario {
        // Tests that a slot request which can be fulfilled will trigger a slot allocation
        TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION,
        // Tests that pending slot requests are tried to be fulfilled upon new slot registrations
        TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION
    }

    private void testRequirementDeclaration(RequirementDeclarationScenario scenario)
            throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceID resourceID = ResourceID.generate();
        final JobID jobId = new JobID();
        final SlotID slotId = new SlotID(resourceID, 0);
        final String targetAddress = "localhost";
        final ResourceProfile resourceProfile = ResourceProfile.fromResources(42.0, 1337);

        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestFuture = new CompletableFuture<>();
        // accept an incoming slot request
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestFuture.complete(
                                            Tuple6.of(
                                                    tuple6.f0, tuple6.f1, tuple6.f2, tuple6.f3,
                                                    tuple6.f4, tuple6.f5));
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
        final SlotReport slotReport = new SlotReport(slotStatus);

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec(
                                resourceManagerId, new TestingResourceAllocatorBuilder().build())) {

            if (scenario
                    == RequirementDeclarationScenario
                            .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION) {
                slotManager.registerTaskManager(
                        taskExecutorConnection,
                        slotReport,
                        ResourceProfile.ANY,
                        ResourceProfile.ANY);
            }

            final ResourceRequirements requirements =
                    ResourceRequirements.create(
                            jobId,
                            targetAddress,
                            Collections.singleton(ResourceRequirement.create(resourceProfile, 1)));
            slotManager.processResourceRequirements(requirements);

            if (scenario
                    == RequirementDeclarationScenario
                            .TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION) {
                slotManager.registerTaskManager(
                        taskExecutorConnection,
                        slotReport,
                        ResourceProfile.ANY,
                        ResourceProfile.ANY);
            }

            assertThat(requestFuture.get())
                    .isEqualTo(
                            Tuple6.of(
                                    slotId,
                                    jobId,
                                    requestFuture.get().f2,
                                    resourceProfile,
                                    targetAddress,
                                    resourceManagerId));

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertThat(slot.getJobId())
                    .as("The slot has not been allocated to the expected allocation id.")
                    .isEqualTo(jobId);
        }
    }

    /** Tests that freeing a slot will correctly reset the slot and mark it as a free slot. */
    @Test
    void testFreeSlot() throws Exception {
        final TaskExecutorConnection taskExecutorConnection = createTaskExecutorConnection();
        final ResourceID resourceID = taskExecutorConnection.getResourceID();
        final SlotID slotId = new SlotID(resourceID, 0);

        final SlotReport slotReport = new SlotReport(createAllocatedSlotStatus(slotId));

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertThat(slot.getState()).isSameAs(SlotState.ALLOCATED);

            slotManager.freeSlot(slotId, new AllocationID());

            assertThat(slot.getState()).isSameAs(SlotState.FREE);

            assertThat(slotManager.getNumberFreeSlots()).isEqualTo(1);
        }
    }

    /**
     * Tests that duplicate resource requirement declaration do not result in additional slots being
     * allocated after a pending slot request has been fulfilled but not yet freed.
     */
    @Test
    void testDuplicateResourceRequirementDeclarationAfterSuccessfulAllocation() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
        final ResourceAllocator resourceAllocator =
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(
                                ignored -> allocateResourceCalls.incrementAndGet())
                        .build();
        ResourceRequirements requirements = createResourceRequirementsForSingleSlot();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

        final ResourceID resourceID = ResourceID.generate();

        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        final SlotID slotId = new SlotID(resourceID, 0);
        final SlotReport slotReport = new SlotReport(createFreeSlotStatus(slotId));

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec(resourceManagerId, resourceAllocator)) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(requirements);

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertThat(slot.getState()).isEqualTo(SlotState.ALLOCATED);

            slotManager.processResourceRequirements(requirements);
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(allocateResourceCalls.get()).isEqualTo(0);
    }

    /**
     * Tests that a slot allocated for one job can be allocated for another job after being freed.
     */
    @Test
    void testSlotCanBeAllocatedForDifferentJobAfterFree() throws Exception {
        testSlotCanBeAllocatedForDifferentJobAfterFree(
                SecondRequirementDeclarationTime.BEFORE_FREE);
        testSlotCanBeAllocatedForDifferentJobAfterFree(SecondRequirementDeclarationTime.AFTER_FREE);
    }

    private enum SecondRequirementDeclarationTime {
        BEFORE_FREE,
        AFTER_FREE
    }

    private void testSlotCanBeAllocatedForDifferentJobAfterFree(
            SecondRequirementDeclarationTime secondRequirementDeclarationTime) throws Exception {
        final AllocationID allocationId = new AllocationID();
        final ResourceRequirements resourceRequirements1 =
                createResourceRequirementsForSingleSlot();
        final ResourceRequirements resourceRequirements2 =
                createResourceRequirementsForSingleSlot();

        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        final ResourceID resourceID = taskManagerConnection.getResourceID();

        final SlotID slotId = new SlotID(resourceID, 0);
        final SlotReport slotReport = new SlotReport(createFreeSlotStatus(slotId));

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(resourceRequirements1);

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertThat(resourceRequirements1.getJobId())
                    .as("The slot has not been allocated to the expected job id.")
                    .isEqualTo(slot.getJobId());

            if (secondRequirementDeclarationTime == SecondRequirementDeclarationTime.BEFORE_FREE) {
                slotManager.processResourceRequirements(resourceRequirements2);
            }

            // clear resource requirements first so that the freed slot isn't immediately
            // re-assigned to the job
            slotManager.processResourceRequirements(
                    ResourceRequirements.create(
                            resourceRequirements1.getJobId(),
                            resourceRequirements1.getTargetAddress(),
                            Collections.emptyList()));
            slotManager.freeSlot(slotId, allocationId);

            if (secondRequirementDeclarationTime == SecondRequirementDeclarationTime.AFTER_FREE) {
                slotManager.processResourceRequirements(resourceRequirements2);
            }

            assertThat(resourceRequirements2.getJobId())
                    .as("The slot has not been allocated to the expected job id.")
                    .isEqualTo(slot.getJobId());
        }
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    void testReceivingUnknownSlotReport() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceAllocator resourceAllocator = new TestingResourceAllocatorBuilder().build();

        final InstanceID unknownInstanceID = new InstanceID();
        final SlotID unknownSlotId = new SlotID(ResourceID.generate(), 0);
        final SlotReport unknownSlotReport = new SlotReport(createFreeSlotStatus(unknownSlotId));

        try (SlotManager slotManager = createSlotManager(resourceManagerId, resourceAllocator)) {
            // check that we don't have any slots registered
            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(0);

            // this should not update anything since the instance id is not known to the slot
            // manager
            assertThat(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport))
                    .isFalse();

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(0);
        }
    }

    /**
     * Tests that slots are updated with respect to the latest incoming slot report. This means that
     * slots for which a report was received are updated accordingly.
     */
    @Test
    void testUpdateSlotReport() throws Exception {
        final TaskExecutorConnection taskManagerConnection = createTaskExecutorConnection();
        final ResourceID resourceId = taskManagerConnection.getResourceID();

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);

        final SlotStatus slotStatus1 = createFreeSlotStatus(slotId1);
        final SlotStatus slotStatus2 = createFreeSlotStatus(slotId2);

        final SlotStatus newSlotStatus2 = createAllocatedSlotStatus(slotId2);
        final JobID jobId = newSlotStatus2.getJobID();

        final SlotReport slotReport1 = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));
        final SlotReport slotReport2 = new SlotReport(Arrays.asList(newSlotStatus2, slotStatus1));

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            // check that we don't have any slots registered
            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(0);

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport1, ResourceProfile.ANY, ResourceProfile.ANY);

            DeclarativeTaskManagerSlot slot1 = slotTracker.getSlot(slotId1);
            DeclarativeTaskManagerSlot slot2 = slotTracker.getSlot(slotId2);

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(2);

            assertThat(slot1.getState()).isSameAs(SlotState.FREE);
            assertThat(slot2.getState()).isSameAs(SlotState.FREE);

            assertThat(
                            slotManager.reportSlotStatus(
                                    taskManagerConnection.getInstanceID(), slotReport2))
                    .isTrue();

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(2);

            assertThat(slotTracker.getSlot(slotId1)).isNotNull();
            assertThat(slotTracker.getSlot(slotId2)).isNotNull();

            // slot1 should still be free, slot2 should have been allocated
            assertThat(slot1.getState()).isSameAs(SlotState.FREE);
            assertThat(jobId).isEqualTo(slotTracker.getSlot(slotId2).getJobId());
        }
    }

    /** Tests that if a slot allocation times out we try to allocate another slot. */
    @Test
    void testSlotAllocationTimeout() throws Exception {
        final CompletableFuture<Void> secondSlotRequestFuture = new CompletableFuture<>();

        final BlockingQueue<Supplier<CompletableFuture<Acknowledge>>> responseQueue =
                new ArrayBlockingQueue<>(2);
        responseQueue.add(
                () -> FutureUtils.completedExceptionally(new TimeoutException("timeout")));
        responseQueue.add(
                () -> {
                    secondSlotRequestFuture.complete(null);
                    return new CompletableFuture<>();
                });

        final TaskExecutorConnection taskManagerConnection =
                createTaskExecutorConnection(
                        new TestingTaskExecutorGatewayBuilder()
                                .setRequestSlotFunction(ignored -> responseQueue.remove().get())
                                .createTestingTaskExecutorGateway());

        final SlotReport slotReport = createSlotReport(taskManagerConnection.getResourceID(), 2);

        final Executor mainThreadExecutor = EXECUTOR_RESOURCE.getExecutor();

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                mainThreadExecutor,
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            CompletableFuture.runAsync(
                            () ->
                                    slotManager.registerTaskManager(
                                            taskManagerConnection,
                                            slotReport,
                                            ResourceProfile.ANY,
                                            ResourceProfile.ANY),
                            mainThreadExecutor)
                    .thenRun(
                            () ->
                                    slotManager.processResourceRequirements(
                                            createResourceRequirementsForSingleSlot()))
                    .get(5, TimeUnit.SECONDS);

            // a second request is only sent if the first request timed out
            secondSlotRequestFuture.get();
        }
    }

    /** Tests that a slot allocation is retried if it times out on the task manager side. */
    @Test
    void testTaskExecutorSlotAllocationTimeoutHandling() throws Exception {
        final JobID jobId = new JobID();
        final ResourceRequirements resourceRequirements =
                createResourceRequirementsForSingleSlot(jobId);
        final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> slotRequestFuture2 = new CompletableFuture<>();
        final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator =
                Arrays.asList(slotRequestFuture1, slotRequestFuture2).iterator();
        final ArrayBlockingQueue<SlotID> slotIds = new ArrayBlockingQueue<>(2);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            slotIds.put(requestSlotParameters.f0);
                                            return slotRequestFutureIterator.next();
                                        }))
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotReport slotReport =
                new SlotReport(
                        Arrays.asList(
                                createFreeSlotStatus(slotId1), createFreeSlotStatus(slotId2)));

        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(resourceRequirements);

            final SlotID firstSlotId = slotIds.take();
            assertThat(slotIds).isEmpty();

            DeclarativeTaskManagerSlot failedSlot = slotTracker.getSlot(firstSlotId);

            // let the first attempt fail --> this should trigger a second attempt
            slotRequestFuture1.completeExceptionally(
                    new SlotAllocationException("Test exception."));

            assertThat(getTotalResourceCount(resourceTracker.getAcquiredResources(jobId)))
                    .isEqualTo(1);

            // the second attempt succeeds
            slotRequestFuture2.complete(Acknowledge.get());

            final SlotID secondSlotId = slotIds.take();
            assertThat(slotIds).isEmpty();

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondSlotId);

            assertThat(slot.getState()).isEqualTo(SlotState.ALLOCATED);
            assertThat(jobId).isEqualTo(slot.getJobId());

            if (!failedSlot.getSlotId().equals(slot.getSlotId())) {
                assertThat(failedSlot.getState()).isEqualTo(SlotState.FREE);
            }
        }
    }

    /**
     * Tests that a pending slot allocation is cancelled if a slot report indicates that the slot is
     * already allocated by another job.
     */
    @Test
    void testSlotReportWithConflictingJobIdDuringSlotAllocation() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();
        final ArrayBlockingQueue<SlotID> requestedSlotIds = new ArrayBlockingQueue<>(2);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            requestedSlotIds.put(requestSlotParameters.f0);
                                            return new CompletableFuture<>();
                                        }))
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                createTaskExecutorConnection(taskExecutorGateway);
        final ResourceID resourceId = taskExecutorConnection.getResourceID();

        final SlotID slotId1 = new SlotID(resourceId, 0);
        final SlotID slotId2 = new SlotID(resourceId, 1);
        final SlotReport slotReport =
                new SlotReport(
                        Arrays.asList(
                                createFreeSlotStatus(slotId1), createFreeSlotStatus(slotId2)));

        final ScheduledExecutor mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder(mainThreadExecutor)
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                mainThreadExecutor,
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            slotManager.registerTaskManager(
                    taskExecutorConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.processResourceRequirements(resourceRequirements);

            final SlotID firstRequestedSlotId = requestedSlotIds.take();
            final SlotID freeSlotId = firstRequestedSlotId.equals(slotId1) ? slotId2 : slotId1;

            final SlotReport newSlotReport =
                    new SlotReport(
                            Arrays.asList(
                                    createAllocatedSlotStatus(firstRequestedSlotId),
                                    createFreeSlotStatus(freeSlotId)));

            slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), newSlotReport);

            final SlotID secondRequestedSlotId = requestedSlotIds.take();

            assertThat(freeSlotId).isEqualTo(secondRequestedSlotId);
        }
    }

    /**
     * Tests that free slots which are reported as allocated won't be considered for fulfilling
     * other pending slot requests.
     *
     * <p>See: FLINK-8505
     */
    @Test
    void testReportAllocatedSlot() throws Exception {
        final ResourceID taskManagerId = ResourceID.generate();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(taskManagerId, taskExecutorGateway);

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            // initially report a single slot as free
            final SlotID slotId = new SlotID(taskManagerId, 0);
            final SlotReport initialSlotReport = new SlotReport(createFreeSlotStatus(slotId));

            slotManager.registerTaskManager(
                    taskExecutorConnection,
                    initialSlotReport,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(1);

            // Now report this slot as allocated
            final SlotStatus slotStatus = createAllocatedSlotStatus(slotId);
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport);

            final JobID jobId = new JobID();
            // this resource requirement should not be fulfilled
            ResourceRequirements requirements = createResourceRequirementsForSingleSlot(jobId);

            slotManager.processResourceRequirements(requirements);

            assertThat(slotTracker.getSlot(slotId).getJobId()).isEqualTo(slotStatus.getJobID());
            assertThat(getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)))
                    .isEqualTo(1);
        }
    }

    /**
     * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
     * fails.
     */
    @Test
    void testSlotRequestFailure() throws Exception {
        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();
        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            ResourceRequirements requirements = createResourceRequirementsForSingleSlot();
            slotManager.processResourceRequirements(requirements);

            final BlockingQueue<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestSlotQueue = new ArrayBlockingQueue<>(1);
            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(2);

            final CompletableFuture<Acknowledge> firstManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(firstManualSlotRequestResponse);
            final CompletableFuture<Acknowledge> secondManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(secondManualSlotRequestResponse);

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                        requestSlotQueue.offer(
                                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
                                        try {
                                            return responseQueue.take();
                                        } catch (InterruptedException ignored) {
                                            return FutureUtils.completedExceptionally(
                                                    new FlinkException(
                                                            "Response queue was interrupted."));
                                        }
                                    })
                            .createTestingTaskExecutorGateway();

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
            final SlotReport slotReport =
                    new SlotReport(createFreeSlotStatus(new SlotID(taskExecutorResourceId, 0)));

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    firstRequest = requestSlotQueue.take();

            // fail first request
            firstManualSlotRequestResponse.completeExceptionally(
                    new SlotAllocationException("Test exception"));

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    secondRequest = requestSlotQueue.take();

            assertThat(secondRequest.f1).isEqualTo(firstRequest.f1);
            assertThat(secondRequest.f0).isEqualTo(firstRequest.f0);

            secondManualSlotRequestResponse.complete(Acknowledge.get());

            final DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondRequest.f0);
            assertThat(slot.getState()).isEqualTo(SlotState.ALLOCATED);
            assertThat(slot.getJobId()).isEqualTo(secondRequest.f1);
        }
    }

    /**
     * Tests that pending request is removed if task executor reports a slot with the same job id.
     */
    @Test
    void testSlotRequestRemovedIfTMReportsAllocation() throws Exception {
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .setSlotTracker(slotTracker)
                        .buildAndStartWithDirectExec()) {

            final JobID jobID = new JobID();
            slotManager.processResourceRequirements(createResourceRequirementsForSingleSlot(jobID));

            final BlockingQueue<
                            Tuple6<
                                    SlotID,
                                    JobID,
                                    AllocationID,
                                    ResourceProfile,
                                    String,
                                    ResourceManagerId>>
                    requestSlotQueue = new ArrayBlockingQueue<>(1);
            final BlockingQueue<CompletableFuture<Acknowledge>> responseQueue =
                    new ArrayBlockingQueue<>(2);

            final CompletableFuture<Acknowledge> firstManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(firstManualSlotRequestResponse);
            final CompletableFuture<Acknowledge> secondManualSlotRequestResponse =
                    new CompletableFuture<>();
            responseQueue.offer(secondManualSlotRequestResponse);

            final TestingTaskExecutorGateway testingTaskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder()
                            .setRequestSlotFunction(
                                    slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                        requestSlotQueue.offer(
                                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6);
                                        try {
                                            return responseQueue.take();
                                        } catch (InterruptedException ignored) {
                                            return FutureUtils.completedExceptionally(
                                                    new FlinkException(
                                                            "Response queue was interrupted."));
                                        }
                                    })
                            .createTestingTaskExecutorGateway();

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(taskExecutorResourceId, testingTaskExecutorGateway);
            final SlotReport slotReport =
                    new SlotReport(createFreeSlotStatus(new SlotID(taskExecutorResourceId, 0)));

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    firstRequest = requestSlotQueue.take();

            // fail first request
            firstManualSlotRequestResponse.completeExceptionally(
                    new TimeoutException("Test exception to fail first allocation"));

            final Tuple6<SlotID, JobID, AllocationID, ResourceProfile, String, ResourceManagerId>
                    secondRequest = requestSlotQueue.take();

            // fail second request
            secondManualSlotRequestResponse.completeExceptionally(
                    new SlotOccupiedException("Test exception", new AllocationID(), jobID));

            assertThat(firstRequest.f1).isEqualTo(jobID);
            assertThat(secondRequest.f1).isEqualTo(jobID);
            assertThat(secondRequest.f0).isEqualTo(firstRequest.f0);

            final DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondRequest.f0);
            assertThat(slot.getState()).isEqualTo(SlotState.ALLOCATED);
            assertThat(slot.getJobId()).isEqualTo(firstRequest.f1);

            assertThat(slotManager.getNumberRegisteredSlots()).isEqualTo(1);
            assertThat(getTotalResourceCount(resourceTracker.getAcquiredResources(jobID)))
                    .isEqualTo(1);
        }
    }

    @Test
    void testTaskExecutorFailedHandling() throws Exception {
        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStartWithDirectExec()) {

            JobID jobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));

            final TaskExecutorConnection taskExecutionConnection1 = createTaskExecutorConnection();
            final SlotReport slotReport1 =
                    createSlotReport(taskExecutionConnection1.getResourceID(), 2);

            slotManager.registerTaskManager(
                    taskExecutionConnection1,
                    slotReport1,
                    ResourceProfile.ANY,
                    ResourceProfile.ANY);

            slotManager.unregisterTaskManager(
                    taskExecutionConnection1.getInstanceID(), TEST_EXCEPTION);

            assertThat(getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)))
                    .isEqualTo(2);
        }
    }

    /**
     * Tests that we only request new resources/containers once we have assigned all pending task
     * manager slots.
     */
    @Test
    void testRequestNewResources() throws Exception {
        final int numberSlots = 2;
        final List<Integer> resourceRequestNumber = new ArrayList<>();
        final TestingResourceAllocator testingResourceAllocator =
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(
                                (resourceDeclarations) -> {
                                    assertThat(resourceDeclarations.size()).isEqualTo(1);
                                    ResourceDeclaration resourceDeclaration =
                                            resourceDeclarations.iterator().next();
                                    resourceRequestNumber.add(resourceDeclaration.getNumNeeded());
                                })
                        .build();

        try (final DeclarativeSlotManager slotManager =
                createSlotManager(
                        ResourceManagerId.generate(), testingResourceAllocator, numberSlots)) {

            final JobID jobId = new JobID();

            // the first 2 requirements should be fulfillable with the pending slots of the first
            // allocation (2 slots per worker)
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
            assertThat(resourceRequestNumber.get(resourceRequestNumber.size() - 1)).isEqualTo(1);

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
            assertThat(resourceRequestNumber.get(resourceRequestNumber.size() - 1)).isEqualTo(1);

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 3));
            assertThat(resourceRequestNumber.get(resourceRequestNumber.size() - 1)).isEqualTo(2);
        }
    }

    private TaskExecutorConnection createTaskExecutorConnection() {
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        return createTaskExecutorConnection(taskExecutorGateway);
    }

    private TaskExecutorConnection createTaskExecutorConnection(
            TaskExecutorGateway taskExecutorGateway) {
        return new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
    }

    /**
     * The spread out slot allocation strategy should spread out the allocated slots across all
     * available TaskExecutors. See FLINK-12122.
     */
    @Test
    void testSpreadOutSlotAllocationStrategy() throws Exception {
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setEvenlySpreadOutSlots(true)
                        .buildAndStartWithDirectExec()) {

            final List<CompletableFuture<JobID>> requestSlotFutures = new ArrayList<>();

            final int numberTaskExecutors = 5;

            // register n TaskExecutors with 2 slots each
            for (int i = 0; i < numberTaskExecutors; i++) {
                final CompletableFuture<JobID> requestSlotFuture = new CompletableFuture<>();
                requestSlotFutures.add(requestSlotFuture);
                registerTaskExecutorWithTwoSlots(slotManager, requestSlotFuture);
            }

            final JobID jobId = new JobID();

            final ResourceRequirements resourceRequirements =
                    createResourceRequirements(jobId, numberTaskExecutors);
            slotManager.processResourceRequirements(resourceRequirements);

            // check that every TaskExecutor has received a slot request
            final Set<JobID> jobIds =
                    new HashSet<>(
                            FutureUtils.combineAll(requestSlotFutures).get(10L, TimeUnit.SECONDS));
            assertThat(jobIds).hasSize(1);
            assertThat(jobIds).containsExactlyInAnyOrder(jobId);
        }
    }

    private void registerTaskExecutorWithTwoSlots(
            DeclarativeSlotManager slotManager, CompletableFuture<JobID> firstRequestSlotFuture) {
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                slotIDJobIDAllocationIDStringResourceManagerIdTuple6 -> {
                                    firstRequestSlotFuture.complete(
                                            slotIDJobIDAllocationIDStringResourceManagerIdTuple6
                                                    .f1);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection firstTaskExecutorConnection =
                createTaskExecutorConnection(taskExecutorGateway);
        final SlotReport firstSlotReport =
                createSlotReport(firstTaskExecutorConnection.getResourceID(), 2);
        slotManager.registerTaskManager(
                firstTaskExecutorConnection,
                firstSlotReport,
                ResourceProfile.ANY,
                ResourceProfile.ANY);
    }

    @Test
    void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(true);
    }

    private static void testNotificationAboutNotEnoughResources(boolean withNotificationGracePeriod)
            throws Exception {
        final JobID jobId = new JobID();
        final int numRequiredSlots = 3;
        final int numExistingSlots = 1;

        List<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotifications =
                new ArrayList<>();

        ResourceEventListener resourceEventListener =
                new TestingResourceEventListenerBuilder()
                        .setNotEnoughResourceAvailableConsumer(
                                (jobId1, acquiredResources) ->
                                        notEnoughResourceNotifications.add(
                                                Tuple2.of(jobId1, acquiredResources)))
                        .build();

        ResourceAllocator resourceAllocator = NonSupportedResourceAllocatorImpl.INSTANCE;

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                new ManuallyTriggeredScheduledExecutor(),
                                resourceAllocator,
                                resourceEventListener)) {

            if (withNotificationGracePeriod) {
                // this should disable notifications
                slotManager.setFailUnfulfillableRequest(false);
            }

            final ResourceID taskExecutorResourceId = ResourceID.generate();
            final TaskExecutorConnection taskExecutionConnection =
                    new TaskExecutorConnection(
                            taskExecutorResourceId,
                            new TestingTaskExecutorGatewayBuilder()
                                    .createTestingTaskExecutorGateway());
            final SlotReport slotReport =
                    createSlotReport(taskExecutorResourceId, numExistingSlots);
            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            ResourceRequirements resourceRequirements =
                    createResourceRequirements(jobId, numRequiredSlots);
            slotManager.processResourceRequirements(resourceRequirements);

            if (withNotificationGracePeriod) {
                assertThat(notEnoughResourceNotifications).isEmpty();

                // re-enable notifications which should also trigger another resource check
                slotManager.setFailUnfulfillableRequest(true);
            }

            assertThat(notEnoughResourceNotifications).hasSize(1);
            Tuple2<JobID, Collection<ResourceRequirement>> notification =
                    notEnoughResourceNotifications.get(0);
            assertThat(notification.f0).isEqualTo(jobId);
            assertThat(notification.f1)
                    .contains(ResourceRequirement.create(ResourceProfile.ANY, numExistingSlots));

            // another slot report that does not indicate any changes should not trigger another
            // notification
            slotManager.reportSlotStatus(taskExecutionConnection.getInstanceID(), slotReport);
            assertThat(notEnoughResourceNotifications).hasSize(1);
        }
    }

    @Test
    void testAllocationUpdatesIgnoredIfTaskExecutorUnregistered() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        // it is important that the returned future is already completed
                        // otherwise it will be cancelled when the task executor is unregistered
                        .setRequestSlotFunction(
                                ignored -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .createTestingTaskExecutorGateway();

        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        System.setSecurityManager(trackingSecurityManager);
        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                executor,
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            JobID jobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));

            final TaskExecutorConnection taskExecutionConnection =
                    createTaskExecutorConnection(taskExecutorGateway);
            final SlotReport slotReport =
                    createSlotReport(taskExecutionConnection.getResourceID(), 1);

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.unregisterTaskManager(
                    taskExecutionConnection.getInstanceID(), TEST_EXCEPTION);

            executor.triggerAll();

            assertThat(trackingSecurityManager.getSystemExitFuture()).isNotDone();
        } finally {
            System.setSecurityManager(null);
        }
    }

    @Test
    void testAllocationUpdatesIgnoredIfSlotMarkedAsAllocatedAfterSlotReport() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        // it is important that the returned future is already completed
                        // otherwise it will be cancelled when the task executor is unregistered
                        .setRequestSlotFunction(
                                ignored -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .createTestingTaskExecutorGateway();

        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        System.setSecurityManager(trackingSecurityManager);
        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                executor,
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            JobID jobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));

            final TaskExecutorConnection taskExecutionConnection =
                    createTaskExecutorConnection(taskExecutorGateway);
            final SlotReport slotReport =
                    createSlotReport(taskExecutionConnection.getResourceID(), 1);

            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.reportSlotStatus(
                    taskExecutionConnection.getInstanceID(),
                    createSlotReportWithAllocatedSlots(
                            taskExecutionConnection.getResourceID(), jobId, 1));

            executor.triggerAll();

            assertThat(trackingSecurityManager.getSystemExitFuture()).isNotDone();
        } finally {
            System.setSecurityManager(null);
        }
    }

    @Test
    void testAllocationUpdatesIgnoredIfSlotMarkedAsPendingForOtherJob() throws Exception {
        final DefaultSlotTracker slotTracker = new DefaultSlotTracker();

        final CompletableFuture<AllocationID> firstSlotAllocationIdFuture =
                new CompletableFuture<>();
        final CompletableFuture<Acknowledge> firstSlotRequestAcknowledgeFuture =
                new CompletableFuture<>();
        final Iterator<CompletableFuture<Acknowledge>> slotRequestAcknowledgeFutures =
                Arrays.asList(
                                firstSlotRequestAcknowledgeFuture,
                                new CompletableFuture<Acknowledge>())
                        .iterator();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                requestSlotParameters -> {
                                    firstSlotAllocationIdFuture.complete(requestSlotParameters.f2);
                                    return slotRequestAcknowledgeFutures.next();
                                })
                        .createTestingTaskExecutorGateway();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotTracker(slotTracker)
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            final TaskExecutorConnection taskExecutionConnection =
                    createTaskExecutorConnection(taskExecutorGateway);
            final SlotReport slotReport =
                    createSlotReport(taskExecutionConnection.getResourceID(), 1);
            final SlotID slotId = Iterators.getOnlyElement(slotReport.iterator()).getSlotID();

            // register task executor
            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);
            slotManager.reportSlotStatus(
                    taskExecutionConnection.getInstanceID(),
                    createSlotReport(taskExecutionConnection.getResourceID(), 1));

            // triggers the allocation of a slot
            final JobID firstJobId = new JobID();
            slotManager.processResourceRequirements(createResourceRequirements(firstJobId, 1));
            // clear requirements immediately to ensure the slot will not get re-allocated to the
            // same job
            slotManager.processResourceRequirements(
                    ResourceRequirements.empty(firstJobId, "foobar"));
            // when the slot is freed it will be re-assigned to this second job
            slotManager.processResourceRequirements(createResourceRequirements(new JobID(), 1));

            slotManager.freeSlot(slotId, firstSlotAllocationIdFuture.get());

            // acknowledge the first allocation
            // this should fail if the acknowledgement is not ignored
            firstSlotRequestAcknowledgeFuture.complete(Acknowledge.get());

            // sanity check that the acknowledge was really ignored
            assertThat(slotTracker.getSlot(slotId).getJobId()).isNotEqualTo(firstJobId);
        }
    }

    @Test
    void testReclaimInactiveSlotsOnClearRequirements() throws Exception {
        final CompletableFuture<JobID> freeInactiveSlotsJobIdFuture = new CompletableFuture<>();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeInactiveSlotsConsumer(freeInactiveSlotsJobIdFuture::complete)
                        .createTestingTaskExecutorGateway();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            final JobID jobId = new JobID();

            final TaskExecutorConnection taskExecutionConnection =
                    createTaskExecutorConnection(taskExecutorGateway);
            final SlotReport slotReport =
                    createSlotReportWithAllocatedSlots(
                            taskExecutionConnection.getResourceID(), jobId, 1);
            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            // setup initial requirements, which should not trigger slots being reclaimed
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
            assertThat(freeInactiveSlotsJobIdFuture).isNotDone();

            // set requirements to 0, which should not trigger slots being reclaimed
            slotManager.processResourceRequirements(ResourceRequirements.empty(jobId, "foobar"));
            assertThat(freeInactiveSlotsJobIdFuture).isNotDone();

            // clear requirements, which should trigger slots being reclaimed
            slotManager.clearResourceRequirements(jobId);
            assertThat(freeInactiveSlotsJobIdFuture.get()).isEqualTo(jobId);
        }
    }

    @Test
    void testProcessResourceRequirementsWithDelay() throws Exception {
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final AtomicInteger allocatedResourceCounter = new AtomicInteger(0);
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final Duration delay = Duration.ofMillis(500);
        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder(scheduledExecutor)
                        .setResourceTracker(resourceTracker)
                        .setRequirementCheckDelay(delay)
                        .setDeclareNeededResourceDelay(delay)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(),
                                new TestingResourceAllocatorBuilder()
                                        .setDeclareResourceNeededConsumer(
                                                (ignored) ->
                                                        allocatedResourceCounter.getAndIncrement())
                                        .build())) {

            final JobID jobId = new JobID();

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
            assertThat(allocatedResourceCounter.get()).isEqualTo(0);
            assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask()).hasSize(1);
            final ScheduledFuture<?> future =
                    scheduledExecutor.getActiveNonPeriodicScheduledTask().iterator().next();
            assertThat(future.getDelay(TimeUnit.MILLISECONDS)).isEqualTo(delay.toMillis());

            // the second request is skipped
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
            assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask()).hasSize(1);

            // trigger checkResourceRequirements
            scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask()).hasSize(1);
            assertThat(allocatedResourceCounter.get()).isEqualTo(0);

            // trigger declareResourceNeeded
            scheduledExecutor.triggerNonPeriodicScheduledTask();
            assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask()).hasSize(0);
            assertThat(allocatedResourceCounter.get()).isEqualTo(1);
        }
    }

    @Test
    void testClearRequirementsClearsResourceTracker() throws Exception {
        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        final CompletableFuture<JobID> freeInactiveSlotsJobIdFuture = new CompletableFuture<>();

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeInactiveSlotsConsumer(freeInactiveSlotsJobIdFuture::complete)
                        .createTestingTaskExecutorGateway();

        try (final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                new TestingResourceAllocatorBuilder().build(),
                                new TestingResourceEventListenerBuilder().build())) {

            final JobID jobId = new JobID();

            final TaskExecutorConnection taskExecutionConnection =
                    createTaskExecutorConnection(taskExecutorGateway);
            final SlotReport slotReport =
                    createSlotReportWithAllocatedSlots(
                            taskExecutionConnection.getResourceID(), jobId, 1);
            slotManager.registerTaskManager(
                    taskExecutionConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
            slotManager.clearResourceRequirements(jobId);

            assertThat(resourceTracker.getMissingResources().keySet()).isEmpty();
        }
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

        final DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotManagerMetricGroup(
                                SlotManagerMetricGroup.create(metricRegistry, "localhost"))
                        .buildAndStartWithDirectExec();

        // sanity check to ensure metrics were actually registered
        assertThat(registeredMetrics.get()).isGreaterThan(0);
        closeFn.accept(slotManager);
        assertThat(registeredMetrics.get()).isEqualTo(0);
    }

    private static SlotReport createSlotReport(ResourceID taskExecutorResourceId, int numberSlots) {
        final Set<SlotStatus> slotStatusSet = new HashSet<>(numberSlots);
        for (int i = 0; i < numberSlots; i++) {
            slotStatusSet.add(createFreeSlotStatus(new SlotID(taskExecutorResourceId, i)));
        }

        return new SlotReport(slotStatusSet);
    }

    private static SlotReport createSlotReportWithAllocatedSlots(
            ResourceID taskExecutorResourceId, JobID jobId, int numberSlots) {
        final Set<SlotStatus> slotStatusSet = new HashSet<>(numberSlots);
        for (int i = 0; i < numberSlots; i++) {
            slotStatusSet.add(
                    createAllocatedSlotStatus(new SlotID(taskExecutorResourceId, i), jobId));
        }

        return new SlotReport(slotStatusSet);
    }

    private static SlotStatus createFreeSlotStatus(SlotID slotId) {
        return new SlotStatus(slotId, ResourceProfile.ANY);
    }

    private static SlotStatus createAllocatedSlotStatus(SlotID slotId) {
        return createAllocatedSlotStatus(slotId, JobID.generate());
    }

    private static SlotStatus createAllocatedSlotStatus(SlotID slotId, JobID jobId) {
        return new SlotStatus(slotId, ResourceProfile.ANY, jobId, new AllocationID());
    }

    private DeclarativeSlotManager createSlotManager(
            ResourceManagerId resourceManagerId, ResourceAllocator resourceAllocator) {
        return createSlotManager(resourceManagerId, resourceAllocator, 1);
    }

    private DeclarativeSlotManager createSlotManager(
            ResourceManagerId resourceManagerId,
            ResourceAllocator resourceAllocator,
            int numSlotsPerWorker) {
        return createDeclarativeSlotManagerBuilder(
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))
                .setNumSlotsPerWorker(numSlotsPerWorker)
                .setRedundantTaskManagerNum(0)
                .buildAndStartWithDirectExec(resourceManagerId, resourceAllocator);
    }

    private static DeclarativeSlotManagerBuilder createDeclarativeSlotManagerBuilder() {
        return createDeclarativeSlotManagerBuilder(
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));
    }

    private static DeclarativeSlotManagerBuilder createDeclarativeSlotManagerBuilder(
            ScheduledExecutor executor) {
        return DeclarativeSlotManagerBuilder.newBuilder(executor)
                .setDefaultWorkerResourceSpec(WORKER_RESOURCE_SPEC);
    }

    private static ResourceRequirements createResourceRequirementsForSingleSlot() {
        return createResourceRequirementsForSingleSlot(new JobID());
    }

    private static ResourceRequirements createResourceRequirementsForSingleSlot(JobID jobId) {
        return createResourceRequirements(jobId, 1);
    }

    private static ResourceRequirements createResourceRequirements(
            JobID jobId, int numRequiredSlots) {
        return ResourceRequirements.create(
                jobId,
                "foobar",
                Collections.singleton(
                        ResourceRequirement.create(ResourceProfile.UNKNOWN, numRequiredSlots)));
    }

    private static int getTotalResourceCount(Collection<ResourceRequirement> resources) {
        if (resources == null) {
            return 0;
        }
        return resources.stream()
                .map(ResourceRequirement::getNumberOfRequiredSlots)
                .reduce(0, Integer::sum);
    }
}
