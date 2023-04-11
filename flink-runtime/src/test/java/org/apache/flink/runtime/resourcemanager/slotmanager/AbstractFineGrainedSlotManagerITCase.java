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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.testutils.SystemExitTrackingSecurityManager;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Cases of {@link FineGrainedSlotManager}. */
abstract class AbstractFineGrainedSlotManagerITCase extends FineGrainedSlotManagerTestBase {

    // ---------------------------------------------------------------------------------------------
    // Requirement declaration
    // ---------------------------------------------------------------------------------------------

    /**
     * Tests that a requirement declaration with no free slots will trigger the resource allocation.
     */
    @Test
    void testRequirementDeclarationWithoutFreeSlotsTriggersWorkerAllocation() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final CompletableFuture<WorkerResourceSpec> allocateResourceFuture =
                new CompletableFuture<>();
        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            assertThat(resourceDeclarations).hasSize(1);
                            ResourceDeclaration resourceDeclaration =
                                    resourceDeclarations.iterator().next();
                            allocateResourceFuture.complete(resourceDeclaration.getSpec());
                        });
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));

                            assertFutureCompleteAndReturn(allocateResourceFuture);
                        });
            }
        };
    }

    /**
     * Tests that resources continue to be considered missing if we cannot allocate more resources.
     */
    @Test
    void testRequirementDeclarationWithResourceAllocationFailure() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final CompletableFuture<Void> allocateResourceFuture = new CompletableFuture<>();
        new Context() {
            {
                resourceAllocatorBuilder
                        .setDeclareResourceNeededConsumer(
                                (resourceDeclarations) -> allocateResourceFuture.complete(null))
                        .setIsSupportedSupplier(() -> false);
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));

                            assertFutureNotComplete(allocateResourceFuture);
                            assertThat(
                                            getTotalResourceCount(
                                                    getResourceTracker()
                                                            .getMissingResources()
                                                            .get(resourceRequirements.getJobId())))
                                    .isEqualTo(1);
                        });
            }
        };
    }

    /** Tests that resource requirements can be fulfilled with resource that are currently free. */
    @Test
    void testRequirementDeclarationWithFreeResource() throws Exception {
        testRequirementDeclaration(
                RequirementDeclarationScenario
                        .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION);
    }

    /**
     * Tests that resource requirements can be fulfilled with resource that are registered after the
     * requirement declaration.
     */
    @Test
    void testRequirementDeclarationWithPendingResource() throws Exception {
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
        final ResourceID resourceID = ResourceID.generate();
        final JobID jobId = new JobID();
        final SlotID slotId = SlotID.getDynamicSlotID(resourceID);
        final String targetAddress = "localhost";
        final ResourceRequirements requirements =
                ResourceRequirements.create(
                        jobId,
                        targetAddress,
                        Collections.singleton(
                                ResourceRequirement.create(DEFAULT_SLOT_RESOURCE_PROFILE, 1)));

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
                                    requestFuture.complete(tuple6);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);

        new Context() {
            {
                runTest(
                        () -> {
                            if (scenario
                                    == RequirementDeclarationScenario
                                            .TASK_EXECUTOR_REGISTRATION_BEFORE_REQUIREMENT_DECLARATION) {
                                runInMainThread(
                                        () ->
                                                getSlotManager()
                                                        .registerTaskManager(
                                                                taskExecutorConnection,
                                                                new SlotReport(),
                                                                DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                DEFAULT_SLOT_RESOURCE_PROFILE));
                            }

                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(requirements));

                            if (scenario
                                    == RequirementDeclarationScenario
                                            .TASK_EXECUTOR_REGISTRATION_AFTER_REQUIREMENT_DECLARATION) {
                                runInMainThread(
                                        () ->
                                                getSlotManager()
                                                        .registerTaskManager(
                                                                taskExecutorConnection,
                                                                new SlotReport(),
                                                                DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                DEFAULT_SLOT_RESOURCE_PROFILE));
                            }

                            assertThat(assertFutureCompleteAndReturn(requestFuture))
                                    .isEqualTo(
                                            Tuple6.of(
                                                    slotId,
                                                    jobId,
                                                    assertFutureCompleteAndReturn(requestFuture).f2,
                                                    DEFAULT_SLOT_RESOURCE_PROFILE,
                                                    targetAddress,
                                                    getResourceManagerId()));

                            final TaskManagerSlotInformation slot =
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(
                                                    assertFutureCompleteAndReturn(requestFuture).f2)
                                            .get();

                            assertThat(assertFutureCompleteAndReturn(requestFuture).f2)
                                    .as(
                                            "The slot has not been allocated to the expected allocation id.")
                                    .isEqualTo(slot.getAllocationId());
                        });
            }
        };
    }

    /**
     * Tests that blocked slots cannot be used to fulfill requirements, will trigger the new
     * resource allocation.
     */
    @Test
    void testRequirementDeclarationWithBlockedSlotsTriggersWorkerAllocation() throws Exception {
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();

        final CompletableFuture<WorkerResourceSpec> allocateResourceFuture =
                new CompletableFuture<>();

        final ResourceID blockedTaskManager = ResourceID.generate();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(blockedTaskManager, taskExecutorGateway);

        new Context() {
            {
                setBlockedTaskManagerChecker(blockedTaskManager::equals);
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            assertThat(resourceDeclarations).hasSize(1);
                            ResourceDeclaration resourceDeclaration =
                                    resourceDeclarations.iterator().next();
                            allocateResourceFuture.complete(resourceDeclaration.getSpec());
                        });
                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskManagerConnection,
                                                        new SlotReport(),
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        getSlotManager()
                                                .processResourceRequirements(resourceRequirements);
                                    });

                            assertFutureCompleteAndReturn(allocateResourceFuture);
                        });
            }
        };
    }

    /**
     * Tests that duplicate resource requirement declaration do not result in additional slots being
     * allocated after a pending slot request has been fulfilled but not yet freed.
     */
    @Test
    void testDuplicateResourceRequirementDeclarationAfterSuccessfulAllocation() throws Exception {
        final AtomicInteger requestCount = new AtomicInteger(0);
        final List<CompletableFuture<Void>> allocateResourceFutures = new ArrayList<>();
        allocateResourceFutures.add(new CompletableFuture<>());
        allocateResourceFutures.add(new CompletableFuture<>());
        final ResourceRequirements requirements = createResourceRequirementsForSingleSlot();

        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            if (!resourceDeclarations.isEmpty()) {
                                assertThat(requestCount.get()).isLessThan(2);
                                allocateResourceFutures
                                        .get(requestCount.getAndIncrement())
                                        .complete(null);
                            }
                        });
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(requirements));
                            assertFutureCompleteAndReturn(allocateResourceFutures.get(0));

                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(requirements));
                            // check that we have only called the resource allocation only for the
                            // first slot request, since the second request is a duplicate
                            assertFutureNotComplete(allocateResourceFutures.get(1));
                        });
            }
        };
    }

    @Test
    void testResourceCanBeAllocatedForDifferentJobWithDeclarationBeforeSlotFree() throws Exception {
        testResourceCanBeAllocatedForDifferentJobAfterFree(
                SecondRequirementDeclarationTime.BEFORE_FREE);
    }

    @Test
    void testResourceCanBeAllocatedForDifferentJobWithDeclarationAfterSlotFree() throws Exception {
        testResourceCanBeAllocatedForDifferentJobAfterFree(
                SecondRequirementDeclarationTime.AFTER_FREE);
    }

    private enum SecondRequirementDeclarationTime {
        BEFORE_FREE,
        AFTER_FREE
    }

    /**
     * Tests that a resource allocated for one job can be allocated for another job after being
     * freed.
     */
    private void testResourceCanBeAllocatedForDifferentJobAfterFree(
            SecondRequirementDeclarationTime secondRequirementDeclarationTime) throws Exception {
        final CompletableFuture<AllocationID> allocationIdFuture1 = new CompletableFuture<>();
        final CompletableFuture<AllocationID> allocationIdFuture2 = new CompletableFuture<>();
        final ResourceRequirements resourceRequirements1 =
                createResourceRequirementsForSingleSlot();
        final ResourceRequirements resourceRequirements2 =
                createResourceRequirementsForSingleSlot();

        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    if (!allocationIdFuture1.isDone()) {
                                        allocationIdFuture1.complete(tuple6.f2);
                                    } else {
                                        allocationIdFuture2.complete(tuple6.f2);
                                    }
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        new Context() {
            {
                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskManagerConnection,
                                                        slotReport,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        getSlotManager()
                                                .processResourceRequirements(resourceRequirements1);
                                    });

                            final AllocationID allocationId1 =
                                    assertFutureCompleteAndReturn(allocationIdFuture1);
                            TaskManagerSlotInformation slot =
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(allocationId1)
                                            .get();

                            assertThat(resourceRequirements1.getJobId())
                                    .as("The slot has not been allocated to the expected job id.")
                                    .isEqualTo(slot.getJobId());

                            if (secondRequirementDeclarationTime
                                    == SecondRequirementDeclarationTime.BEFORE_FREE) {
                                runInMainThread(
                                        () ->
                                                getSlotManager()
                                                        .processResourceRequirements(
                                                                resourceRequirements2));
                            }

                            // clear resource requirements first so that the freed slot isn't
                            // immediately re-assigned to the job
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        ResourceRequirements.create(
                                                                resourceRequirements1.getJobId(),
                                                                resourceRequirements1
                                                                        .getTargetAddress(),
                                                                Collections.emptyList()));
                                        getSlotManager()
                                                .freeSlot(
                                                        SlotID.getDynamicSlotID(resourceID),
                                                        allocationId1);
                                    });

                            if (secondRequirementDeclarationTime
                                    == SecondRequirementDeclarationTime.AFTER_FREE) {
                                runInMainThread(
                                        () ->
                                                getSlotManager()
                                                        .processResourceRequirements(
                                                                resourceRequirements2));
                            }

                            slot =
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(
                                                    assertFutureCompleteAndReturn(
                                                            allocationIdFuture2))
                                            .get();
                            assertThat(resourceRequirements2.getJobId())
                                    .as("The slot has not been allocated to the expected job id.")
                                    .isEqualTo(slot.getJobId());
                        });
            }
        };
    }

    @Test
    void testRegisterPendingResourceAfterClearingRequirement() throws Exception {
        final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
        final CompletableFuture<Void> allocateResourceFutures = new CompletableFuture<>();
        final CompletableFuture<Void> registerFuture = new CompletableFuture<>();
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    allocationIdFuture.complete(tuple6.f2);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();
        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (ignored) -> allocateResourceFutures.complete(null));
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));
                            assertFutureCompleteAndReturn(allocateResourceFutures);
                            assertThat(getResourceTracker().getMissingResources()).hasSize(1);

                            runInMainThreadAndWait(
                                    () ->
                                            getSlotManager()
                                                    .clearResourceRequirements(
                                                            resourceRequirements.getJobId()));
                            assertThat(getResourceTracker().getMissingResources()).isEmpty();

                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskManagerConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        registerFuture.complete(null);
                                    });
                            assertFutureCompleteAndReturn(registerFuture);
                            assertFutureNotComplete(allocationIdFuture);
                            assertThat(getTaskManagerTracker().getPendingTaskManagers()).isEmpty();
                        });
            }
        };
    }

    @Test
    void testRegisterPendingResourceAfterEmptyResourceRequirement() throws Exception {
        final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
        final CompletableFuture<Void> allocateResourceFutures = new CompletableFuture<>();
        final CompletableFuture<Void> registerFuture = new CompletableFuture<>();
        final ResourceRequirements resourceRequirements = createResourceRequirementsForSingleSlot();
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    allocationIdFuture.complete(tuple6.f2);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceID = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceID, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();
        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (ignored) -> allocateResourceFutures.complete(null));
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));
                            assertFutureCompleteAndReturn(allocateResourceFutures);
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        ResourceRequirements.empty(
                                                                resourceRequirements.getJobId(),
                                                                resourceRequirements
                                                                        .getTargetAddress()));
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskManagerConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                        registerFuture.complete(null);
                                    });
                            assertFutureCompleteAndReturn(registerFuture);
                            assertFutureNotComplete(allocationIdFuture);
                            assertThat(getTaskManagerTracker().getPendingTaskManagers()).isEmpty();
                        });
            }
        };
    }

    /**
     * Tests that we only request new resources/containers once we have assigned all pending task
     * managers.
     */
    @Test
    void testRequestNewResources() throws Exception {
        final JobID jobId = new JobID();
        final AtomicInteger requestCount = new AtomicInteger(0);
        final List<CompletableFuture<Void>> allocateResourceFutures = new ArrayList<>();
        allocateResourceFutures.add(new CompletableFuture<>());
        allocateResourceFutures.add(new CompletableFuture<>());

        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            if (!resourceDeclarations.isEmpty()) {
                                assertThat(requestCount.get()).isLessThan(2);
                                allocateResourceFutures
                                        .get(requestCount.getAndIncrement())
                                        .complete(null);
                            }
                        });
                runTest(
                        () -> {
                            // the first requirements should be fulfillable with the pending task
                            // managers of the first allocation
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(
                                                                    jobId,
                                                                    DEFAULT_NUM_SLOTS_PER_WORKER)));
                            assertFutureCompleteAndReturn(allocateResourceFutures.get(0));
                            assertFutureNotComplete(allocateResourceFutures.get(1));

                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(
                                                                    jobId,
                                                                    DEFAULT_NUM_SLOTS_PER_WORKER
                                                                            + 1)));
                            assertFutureCompleteAndReturn(allocateResourceFutures.get(1));
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Slot allocation failure handling
    // ---------------------------------------------------------------------------------------------

    /** Tests that if a slot allocation times out we try to allocate another slot. */
    @Test
    void testSlotRequestTimeout() throws Exception {
        testSlotRequestFailureWithException(new TimeoutException("timeout"));
    }

    /**
     * Tests that the SlotManager retries allocating a slot if the TaskExecutor#requestSlot call
     * fails.
     */
    @Test
    void testSlotRequestFailure() throws Exception {
        testSlotRequestFailureWithException(new SlotAllocationException("Test exception."));
    }

    void testSlotRequestFailureWithException(Exception exception) throws Exception {
        final JobID jobId = new JobID();
        final ResourceRequirements resourceRequirements =
                createResourceRequirementsForSingleSlot(jobId);
        final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> slotRequestFuture2 =
                CompletableFuture.completedFuture(Acknowledge.get());
        final Iterator<CompletableFuture<Acknowledge>> slotRequestFutureIterator =
                Arrays.asList(slotRequestFuture1, slotRequestFuture2).iterator();
        final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(2);

        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                FunctionUtils.uncheckedFunction(
                                        requestSlotParameters -> {
                                            allocationIds.put(requestSlotParameters.f2);
                                            return slotRequestFutureIterator.next();
                                        }))
                        .createTestingTaskExecutorGateway();

        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        new Context() {
            {
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
                                                .processResourceRequirements(resourceRequirements);
                                    });

                            final AllocationID firstAllocationId = allocationIds.take();
                            assertThat(allocationIds).isEmpty();

                            // let the first attempt fail --> this should trigger a second attempt
                            runInMainThread(
                                    () -> slotRequestFuture1.completeExceptionally(exception));

                            final AllocationID secondAllocationId = allocationIds.take();
                            assertThat(allocationIds).isEmpty();

                            final TaskManagerSlotInformation slot =
                                    getTaskManagerTracker()
                                            .getAllocatedOrPendingSlot(secondAllocationId)
                                            .get();

                            assertThat(slot.getJobId()).isEqualTo(jobId);

                            assertThat(
                                            getTaskManagerTracker()
                                                    .getAllocatedOrPendingSlot(firstAllocationId))
                                    .isNotPresent();
                        });
            }
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Allocation update
    // ---------------------------------------------------------------------------------------------

    /**
     * Verify that the ack of request slot form unregistered task manager will not cause system
     * breakdown.
     */
    @Test
    void testAllocationUpdatesIgnoredIfTaskExecutorUnregistered() throws Exception {
        final CompletableFuture<Acknowledge> slotRequestFuture = new CompletableFuture<>();
        final CompletableFuture<Void> slotRequestCallFuture = new CompletableFuture<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                ignored -> {
                                    slotRequestCallFuture.complete(null);
                                    return slotRequestFuture;
                                })
                        .createTestingTaskExecutorGateway();

        // The fatal error handler will exit the system if there is any exceptions in handling the
        // ack of request slot. We need the security manager to verify that would not happen.
        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        System.setSecurityManager(trackingSecurityManager);

        final JobID jobId = new JobID();
        final ResourceID taskExecutorResourceId = ResourceID.generate();
        final TaskExecutorConnection taskExecutionConnection =
                new TaskExecutorConnection(taskExecutorResourceId, taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        new Context() {
            {
                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(jobId, 1));
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutionConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                    });
                            assertFutureCompleteAndReturn(slotRequestCallFuture);

                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .unregisterTaskManager(
                                                        taskExecutionConnection.getInstanceID(),
                                                        TEST_EXCEPTION);
                                        slotRequestFuture.complete(Acknowledge.get());
                                    });

                            assertThat(trackingSecurityManager.getSystemExitFuture()).isNotDone();
                        });
            }
        };

        System.setSecurityManager(null);
    }

    @Test
    void testAllocationUpdatesIgnoredIfSlotMarkedAsAllocatedAfterSlotReport() throws Exception {
        final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        // it is important that the returned future is already completed
                        // otherwise it will be cancelled when the task executor is unregistered
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    allocationIdFuture.complete(tuple6.f2);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .createTestingTaskExecutorGateway();

        // The fatal error handler will exit the system if there is any exceptions in handling the
        // ack of request slot. We need the security manager to verify that would not happen.
        final SystemExitTrackingSecurityManager trackingSecurityManager =
                new SystemExitTrackingSecurityManager();
        System.setSecurityManager(trackingSecurityManager);

        final TaskExecutorConnection taskExecutionConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final SlotReport slotReport = new SlotReport();

        new Context() {
            {
                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        getSlotManager()
                                                .processResourceRequirements(
                                                        createResourceRequirements(new JobID(), 1));
                                        getSlotManager()
                                                .registerTaskManager(
                                                        taskExecutionConnection,
                                                        slotReport,
                                                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                        DEFAULT_SLOT_RESOURCE_PROFILE);
                                    });

                            final AllocationID allocationId =
                                    assertFutureCompleteAndReturn(allocationIdFuture);
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .reportSlotStatus(
                                                            taskExecutionConnection.getInstanceID(),
                                                            new SlotReport(
                                                                    createAllocatedSlotStatus(
                                                                            new JobID(),
                                                                            allocationId,
                                                                            DEFAULT_SLOT_RESOURCE_PROFILE))));

                            assertThat(trackingSecurityManager.getSystemExitFuture()).isNotDone();
                        });
            }
        };

        System.setSecurityManager(null);
    }
}
