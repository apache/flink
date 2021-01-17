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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
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
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import akka.pattern.AskTimeoutException;
import org.junit.Test;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DeclarativeSlotManager}. */
public class DeclarativeSlotManagerTest extends TestLogger {

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
    public void testCloseAfterSuspendDoesNotThrowException() throws Exception {
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder().buildAndStartWithDirectExec()) {
            slotManager.suspend();
        }
    }

    /** Tests that we can register task manager and their slots at the slot manager. */
    @Test
    public void testTaskManagerRegistration() throws Exception {
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

            assertThat(
                    "The number registered slots does not equal the expected number.",
                    slotManager.getNumberRegisteredSlots(),
                    is(2));

            assertNotNull(slotTracker.getSlot(slotId1));
            assertNotNull(slotTracker.getSlot(slotId2));
        }
    }

    /** Tests that un-registration of task managers will free and remove all registered slots. */
    @Test
    public void testTaskManagerUnregistration() throws Exception {
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

            assertEquals(
                    "The number registered slots does not equal the expected number.",
                    2,
                    slotManager.getNumberRegisteredSlots());

            slotManager.processResourceRequirements(resourceRequirements);

            slotManager.unregisterTaskManager(
                    taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

            assertEquals(0, slotManager.getNumberRegisteredSlots());
        }
    }

    /** Tests that a slot request with no free slots will trigger the resource allocation. */
    @Test
    public void testRequirementDeclarationWithoutFreeSlotsTriggersWorkerAllocation()
            throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        CompletableFuture<WorkerResourceSpec> allocateResourceFuture = new CompletableFuture<>();
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(allocateResourceFuture::complete)
                        .build();

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {

            slotManager.processResourceRequirements(resourceRequirements);

            allocateResourceFuture.get();
        }
    }

    /**
     * Tests that resources continue to be considered missing if we cannot allocate more resources.
     */
    @Test
    public void testRequirementDeclarationWithResourceAllocationFailure() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(value -> false)
                        .build();

        final ResourceTracker resourceTracker = new DefaultResourceTracker();

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setResourceTracker(resourceTracker)
                        .buildAndStartWithDirectExec(
                                ResourceManagerId.generate(), resourceManagerActions)) {

            slotManager.processResourceRequirements(resourceRequirements);

            final JobID jobId = resourceRequirements.getJobId();
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(1));
        }
    }

    /** Tests that resource requirements can be fulfilled with slots that are currently free. */
    @Test
    public void testRequirementDeclarationWithFreeSlot() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION);
    }

    /**
     * Tests that resource requirements can be fulfilled with slots that are registered after the
     * requirement declaration.
     */
    @Test
    public void testRequirementDeclarationWithPendingSlot() throws Exception {
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
                                resourceManagerId, new TestingResourceActionsBuilder().build())) {

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

            assertThat(
                    requestFuture.get(),
                    is(
                            equalTo(
                                    Tuple6.of(
                                            slotId,
                                            jobId,
                                            requestFuture.get().f2,
                                            resourceProfile,
                                            targetAddress,
                                            resourceManagerId))));

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertEquals(
                    "The slot has not been allocated to the expected allocation id.",
                    jobId,
                    slot.getJobId());
        }
    }

    /** Tests that freeing a slot will correctly reset the slot and mark it as a free slot. */
    @Test
    public void testFreeSlot() throws Exception {
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

            assertSame(SlotState.ALLOCATED, slot.getState());

            slotManager.freeSlot(slotId, new AllocationID());

            assertSame(SlotState.FREE, slot.getState());

            assertEquals(1, slotManager.getNumberFreeSlots());
        }
    }

    /**
     * Tests that duplicate resource requirement declaration do not result in additional slots being
     * allocated after a pending slot request has been fulfilled but not yet freed.
     */
    @Test
    public void testDuplicateResourceRequirementDeclarationAfterSuccessfulAllocation()
            throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
        final ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceConsumer(
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
                        .buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport, ResourceProfile.ANY, ResourceProfile.ANY);

            slotManager.processResourceRequirements(requirements);

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(slotId);

            assertThat(slot.getState(), is(SlotState.ALLOCATED));

            slotManager.processResourceRequirements(requirements);
        }

        // check that we have only called the resource allocation only for the first slot request,
        // since the second request is a duplicate
        assertThat(allocateResourceCalls.get(), is(0));
    }

    /**
     * Tests that a slot allocated for one job can be allocated for another job after being freed.
     */
    @Test
    public void testSlotCanBeAllocatedForDifferentJobAfterFree() throws Exception {
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

            assertEquals(
                    "The slot has not been allocated to the expected job id.",
                    resourceRequirements1.getJobId(),
                    slot.getJobId());

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

            assertEquals(
                    "The slot has not been allocated to the expected job id.",
                    resourceRequirements2.getJobId(),
                    slot.getJobId());
        }
    }

    /**
     * Tests that the slot manager ignores slot reports of unknown origin (not registered task
     * managers).
     */
    @Test
    public void testReceivingUnknownSlotReport() throws Exception {
        final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

        final InstanceID unknownInstanceID = new InstanceID();
        final SlotID unknownSlotId = new SlotID(ResourceID.generate(), 0);
        final SlotReport unknownSlotReport = new SlotReport(createFreeSlotStatus(unknownSlotId));

        try (SlotManager slotManager =
                createSlotManager(resourceManagerId, resourceManagerActions)) {
            // check that we don't have any slots registered
            assertThat(slotManager.getNumberRegisteredSlots(), is(0));

            // this should not update anything since the instance id is not known to the slot
            // manager
            assertFalse(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport));

            assertThat(slotManager.getNumberRegisteredSlots(), is(0));
        }
    }

    /**
     * Tests that slots are updated with respect to the latest incoming slot report. This means that
     * slots for which a report was received are updated accordingly.
     */
    @Test
    public void testUpdateSlotReport() throws Exception {
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
            assertEquals(0, slotManager.getNumberRegisteredSlots());

            slotManager.registerTaskManager(
                    taskManagerConnection, slotReport1, ResourceProfile.ANY, ResourceProfile.ANY);

            DeclarativeTaskManagerSlot slot1 = slotTracker.getSlot(slotId1);
            DeclarativeTaskManagerSlot slot2 = slotTracker.getSlot(slotId2);

            assertEquals(2, slotManager.getNumberRegisteredSlots());

            assertSame(SlotState.FREE, slot1.getState());
            assertSame(SlotState.FREE, slot2.getState());

            assertTrue(
                    slotManager.reportSlotStatus(
                            taskManagerConnection.getInstanceID(), slotReport2));

            assertEquals(2, slotManager.getNumberRegisteredSlots());

            assertNotNull(slotTracker.getSlot(slotId1));
            assertNotNull(slotTracker.getSlot(slotId2));

            // slot1 should still be free, slot2 should have been allocated
            assertSame(SlotState.FREE, slot1.getState());
            assertEquals(jobId, slotTracker.getSlot(slotId2).getJobId());
        }
    }

    /** Tests that if a slot allocation times out we try to allocate another slot. */
    @Test
    public void testSlotAllocationTimeout() throws Exception {
        final CompletableFuture<Void> secondSlotRequestFuture = new CompletableFuture<>();

        final BlockingQueue<Supplier<CompletableFuture<Acknowledge>>> responseQueue =
                new ArrayBlockingQueue<>(2);
        responseQueue.add(
                () -> FutureUtils.completedExceptionally(new AskTimeoutException("timeout")));
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

        final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

        try (DeclarativeSlotManager slotManager = createDeclarativeSlotManagerBuilder().build()) {

            slotManager.start(
                    ResourceManagerId.generate(),
                    mainThreadExecutor,
                    new TestingResourceActionsBuilder().build());

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
    public void testTaskExecutorSlotAllocationTimeoutHandling() throws Exception {
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
            assertThat(slotIds, is(empty()));

            DeclarativeTaskManagerSlot failedSlot = slotTracker.getSlot(firstSlotId);

            // let the first attempt fail --> this should trigger a second attempt
            slotRequestFuture1.completeExceptionally(
                    new SlotAllocationException("Test exception."));

            assertThat(getTotalResourceCount(resourceTracker.getAcquiredResources(jobId)), is(1));

            // the second attempt succeeds
            slotRequestFuture2.complete(Acknowledge.get());

            final SlotID secondSlotId = slotIds.take();
            assertThat(slotIds, is(empty()));

            DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondSlotId);

            assertThat(slot.getState(), is(SlotState.ALLOCATED));
            assertEquals(jobId, slot.getJobId());

            if (!failedSlot.getSlotId().equals(slot.getSlotId())) {
                assertThat(failedSlot.getState(), is(SlotState.FREE));
            }
        }
    }

    /**
     * Tests that a pending slot allocation is cancelled if a slot report indicates that the slot is
     * already allocated by another job.
     */
    @Test
    public void testSlotReportWithConflictingJobIdDuringSlotAllocation() throws Exception {
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
                createDeclarativeSlotManagerBuilder()
                        .setScheduledExecutor(mainThreadExecutor)
                        .build()) {

            slotManager.start(
                    ResourceManagerId.generate(),
                    mainThreadExecutor,
                    new TestingResourceActionsBuilder().build());

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

            assertEquals(freeSlotId, secondRequestedSlotId);
        }
    }

    /**
     * Tests that free slots which are reported as allocated won't be considered for fulfilling
     * other pending slot requests.
     *
     * <p>See: FLINK-8505
     */
    @Test
    public void testReportAllocatedSlot() throws Exception {
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

            assertThat(slotManager.getNumberRegisteredSlots(), is(equalTo(1)));

            // Now report this slot as allocated
            final SlotStatus slotStatus = createAllocatedSlotStatus(slotId);
            final SlotReport slotReport = new SlotReport(slotStatus);

            slotManager.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport);

            final JobID jobId = new JobID();
            // this resource requirement should not be fulfilled
            ResourceRequirements requirements = createResourceRequirementsForSingleSlot(jobId);

            slotManager.processResourceRequirements(requirements);

            assertThat(slotTracker.getSlot(slotId).getJobId(), is(slotStatus.getJobID()));
            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(1));
        }
    }

    /**
     * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
     * fails.
     */
    @Test
    public void testSlotRequestFailure() throws Exception {
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

            assertThat(secondRequest.f1, equalTo(firstRequest.f1));
            assertThat(secondRequest.f0, equalTo(firstRequest.f0));

            secondManualSlotRequestResponse.complete(Acknowledge.get());

            final DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondRequest.f0);
            assertThat(slot.getState(), equalTo(SlotState.ALLOCATED));
            assertThat(slot.getJobId(), equalTo(secondRequest.f1));
        }
    }

    /**
     * Tests that pending request is removed if task executor reports a slot with the same job id.
     */
    @Test
    public void testSlotRequestRemovedIfTMReportsAllocation() throws Exception {
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

            assertThat(firstRequest.f1, equalTo(jobID));
            assertThat(secondRequest.f1, equalTo(jobID));
            assertThat(secondRequest.f0, equalTo(firstRequest.f0));

            final DeclarativeTaskManagerSlot slot = slotTracker.getSlot(secondRequest.f0);
            assertThat(slot.getState(), equalTo(SlotState.ALLOCATED));
            assertThat(slot.getJobId(), equalTo(firstRequest.f1));

            assertThat(slotManager.getNumberRegisteredSlots(), is(1));
            assertThat(getTotalResourceCount(resourceTracker.getAcquiredResources(jobID)), is(1));
        }
    }

    @Test
    public void testTaskExecutorFailedHandling() throws Exception {
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

            assertThat(
                    getTotalResourceCount(resourceTracker.getMissingResources().get(jobId)), is(2));
        }
    }

    /**
     * Tests that we only request new resources/containers once we have assigned all pending task
     * manager slots.
     */
    @Test
    public void testRequestNewResources() throws Exception {
        final int numberSlots = 2;
        final AtomicInteger resourceRequests = new AtomicInteger(0);
        final TestingResourceActions testingResourceActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(
                                ignored -> {
                                    resourceRequests.incrementAndGet();
                                    return true;
                                })
                        .build();

        try (final DeclarativeSlotManager slotManager =
                createSlotManager(
                        ResourceManagerId.generate(), testingResourceActions, numberSlots)) {

            final JobID jobId = new JobID();

            // the first 2 requirements should be fulfillable with the pending slots of the first
            // allocation (2 slots per worker)
            slotManager.processResourceRequirements(createResourceRequirements(jobId, 1));
            assertThat(resourceRequests.get(), is(1));

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 2));
            assertThat(resourceRequests.get(), is(1));

            slotManager.processResourceRequirements(createResourceRequirements(jobId, 3));
            assertThat(resourceRequests.get(), is(2));
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
    public void testSpreadOutSlotAllocationStrategy() throws Exception {
        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .setSlotMatchingStrategy(LeastUtilizationSlotMatchingStrategy.INSTANCE)
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
            assertThat(jobIds, hasSize(1));
            assertThat(jobIds, containsInAnyOrder(jobId));
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
    public void testNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(false);
    }

    @Test
    public void testGracePeriodForNotificationAboutNotEnoughResources() throws Exception {
        testNotificationAboutNotEnoughResources(true);
    }

    private static void testNotificationAboutNotEnoughResources(boolean withNotificationGracePeriod)
            throws Exception {
        final JobID jobId = new JobID();
        final int numRequiredSlots = 3;
        final int numExistingSlots = 1;

        List<Tuple2<JobID, Collection<ResourceRequirement>>> notEnoughResourceNotifications =
                new ArrayList<>();
        ResourceActions resourceManagerActions =
                new TestingResourceActionsBuilder()
                        .setAllocateResourceFunction(ignored -> false)
                        .setNotEnoughResourcesConsumer(
                                (jobId1, acquiredResources) ->
                                        notEnoughResourceNotifications.add(
                                                Tuple2.of(jobId1, acquiredResources)))
                        .build();

        try (DeclarativeSlotManager slotManager =
                createDeclarativeSlotManagerBuilder()
                        .buildAndStart(
                                ResourceManagerId.generate(),
                                new ManuallyTriggeredScheduledExecutor(),
                                resourceManagerActions)) {

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
                assertThat(notEnoughResourceNotifications, empty());

                // re-enable notifications which should also trigger another resource check
                slotManager.setFailUnfulfillableRequest(true);
            }

            assertThat(notEnoughResourceNotifications, hasSize(1));
            Tuple2<JobID, Collection<ResourceRequirement>> notification =
                    notEnoughResourceNotifications.get(0);
            assertThat(notification.f0, is(jobId));
            assertThat(
                    notification.f1,
                    hasItem(ResourceRequirement.create(ResourceProfile.ANY, numExistingSlots)));
        }
    }

    @Test
    public void testAllocationUpdatesIgnoredIfTaskExecutorUnregistered() throws Exception {
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
                                new TestingResourceActionsBuilder().build())) {

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

            assertThat(trackingSecurityManager.getSystemExitFuture().isDone(), is(false));
        } finally {
            System.setSecurityManager(null);
        }
    }

    @Test
    public void testAllocationUpdatesIgnoredIfSlotMarkedAsAllocatedAfterSlotReport()
            throws Exception {
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
                                new TestingResourceActionsBuilder().build())) {

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

            assertThat(trackingSecurityManager.getSystemExitFuture().isDone(), is(false));
        } finally {
            System.setSecurityManager(null);
        }
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
            ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
        return createSlotManager(resourceManagerId, resourceManagerActions, 1);
    }

    private DeclarativeSlotManager createSlotManager(
            ResourceManagerId resourceManagerId,
            ResourceActions resourceManagerActions,
            int numSlotsPerWorker) {
        return createDeclarativeSlotManagerBuilder()
                .setNumSlotsPerWorker(numSlotsPerWorker)
                .setRedundantTaskManagerNum(0)
                .buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions);
    }

    private static DeclarativeSlotManagerBuilder createDeclarativeSlotManagerBuilder() {
        return DeclarativeSlotManagerBuilder.newBuilder()
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
