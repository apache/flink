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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link FineGrainedTaskManagerTracker}. */
class FineGrainedTaskManagerTrackerTest {
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    static final WorkerResourceSpec DEFAULT_WORKER_RESOURCE_SPEC =
            new WorkerResourceSpec.Builder()
                    .setCpuCores(10.0)
                    .setTaskHeapMemoryMB(1000)
                    .setTaskOffHeapMemoryMB(1000)
                    .setNetworkMemoryMB(1000)
                    .setManagedMemoryMB(1000)
                    .build();
    static final int DEFAULT_NUM_SLOTS_PER_WORKER = 2;
    static final ResourceProfile DEFAULT_TOTAL_RESOURCE_PROFILE =
            SlotManagerUtils.generateTaskManagerTotalResourceProfile(DEFAULT_WORKER_RESOURCE_SPEC);
    static final ResourceProfile DEFAULT_SLOT_RESOURCE_PROFILE =
            SlotManagerUtils.generateDefaultSlotResourceProfile(
                    DEFAULT_WORKER_RESOURCE_SPEC, DEFAULT_NUM_SLOTS_PER_WORKER);

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testInitState() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();
        assertThat(taskManagerTracker.getPendingTaskManagers()).isEmpty();
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).isEmpty();
    }

    @Test
    void testAllocateTaskManagersAccordingToResultWithNoResourceAllocator() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createTaskManagerTracker();

        taskManagerTracker.initialize(
                NonSupportedResourceAllocatorImpl.INSTANCE, EXECUTOR_RESOURCE.getExecutor());

        PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        ResourceAllocationResult result =
                new ResourceAllocationResult.Builder()
                        .addPendingTaskManagerAllocate(pendingTaskManager)
                        .addAllocationOnPendingResource(
                                new JobID(),
                                pendingTaskManager.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .build();

        Set<PendingTaskManagerId> failedAllocations =
                taskManagerTracker.allocateTaskManagersAccordingTo(result);
        assertThat(failedAllocations).containsExactly(pendingTaskManager.getPendingTaskManagerId());
    }

    @Test
    void testAllocateTaskManagersAccordingToResult() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createTaskManagerTracker();
        CompletableFuture<ResourceDeclaration> declarationFuture = new CompletableFuture<>();
        Consumer<Collection<ResourceDeclaration>> declareResourceNeededConsumer =
                resourceDeclarations -> {
                    assertThat(resourceDeclarations).hasSize(1);
                    declarationFuture.complete(resourceDeclarations.stream().findFirst().get());
                };

        PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        taskManagerTracker.initialize(
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(declareResourceNeededConsumer)
                        .build(),
                EXECUTOR_RESOURCE.getExecutor());
        ResourceAllocationResult result =
                new ResourceAllocationResult.Builder()
                        .addPendingTaskManagerAllocate(pendingTaskManager)
                        .addAllocationOnPendingResource(
                                new JobID(),
                                pendingTaskManager.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .build();

        Set<PendingTaskManagerId> failedAllocations =
                taskManagerTracker.allocateTaskManagersAccordingTo(result);
        assertThat(failedAllocations).isEmpty();
        assertThat(declarationFuture)
                .isCompletedWithValue(
                        new ResourceDeclaration(
                                DEFAULT_WORKER_RESOURCE_SPEC, 1, Collections.emptySet()));
        assertThat(taskManagerTracker.getPendingTaskManagers()).hasSize(1);

        boolean registered =
                taskManagerTracker.registerTaskManager(
                        TASK_EXECUTOR_CONNECTION,
                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                        DEFAULT_SLOT_RESOURCE_PROFILE,
                        pendingTaskManager.getPendingTaskManagerId());
        assertThat(registered).isTrue();
        assertThat(taskManagerTracker.getPendingTaskManagers()).hasSize(0);
    }

    @Test
    void testRegisterAndUnregisterTaskManager() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();

        // Add task manager
        taskManagerTracker.registerTaskManager(
                TASK_EXECUTOR_CONNECTION,
                DEFAULT_TOTAL_RESOURCE_PROFILE,
                DEFAULT_SLOT_RESOURCE_PROFILE,
                null);
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).hasSize(1);
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .isPresent();

        // Remove task manager
        taskManagerTracker.unregisterTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID());
        assertThat(taskManagerTracker.getRegisteredTaskManagers()).isEmpty();
    }

    @Test
    void testUnregisterUnknownTaskManager() {
        assertThatThrownBy(
                        () -> {
                            final FineGrainedTaskManagerTracker taskManagerTracker =
                                    createAndStartTaskManagerTracker();
                            taskManagerTracker.unregisterTaskManager(new InstanceID());
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSlotAllocation() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.registerTaskManager(
                TASK_EXECUTOR_CONNECTION, totalResource, totalResource, null);
        // Notify free slot is now pending
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.PENDING);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(7, 800)));

        // Notify pending slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(7, 800)));

        // Notify free slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.ALLOCATED);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(5, 500)));
    }

    @Test
    void testFreeSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.registerTaskManager(
                TASK_EXECUTOR_CONNECTION, totalResource, totalResource, null);
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.PENDING);
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.ALLOCATED);

        // Free pending slot
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.FREE);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1)).isNotPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(ResourceProfile.fromResources(8, 700)));
        // Free allocated slot
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.FREE);
        assertThat(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2)).isNotPresent();
        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getAvailableResource())
                                        .isEqualTo(totalResource));
    }

    @Test
    void testFreeUnknownSlot() {
        assertThatThrownBy(
                        () -> {
                            final FineGrainedTaskManagerTracker taskManagerTracker =
                                    createAndStartTaskManagerTracker();

                            taskManagerTracker.notifySlotStatus(
                                    new AllocationID(),
                                    new JobID(),
                                    new InstanceID(),
                                    ResourceProfile.ANY,
                                    SlotState.FREE);
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testClearPendingAllocations() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();

        JobID job1 = new JobID();
        JobID job2 = new JobID();
        PendingTaskManager pendingTaskManagerJob1 =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        PendingTaskManager pendingTaskManagerJob2 =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        ResourceAllocationResult result =
                new ResourceAllocationResult.Builder()
                        .addPendingTaskManagerAllocate(pendingTaskManagerJob1)
                        .addAllocationOnPendingResource(
                                job1,
                                pendingTaskManagerJob1.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .addPendingTaskManagerAllocate(pendingTaskManagerJob2)
                        .addAllocationOnPendingResource(
                                job2,
                                pendingTaskManagerJob2.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .build();

        Set<PendingTaskManagerId> failedAllocations =
                taskManagerTracker.allocateTaskManagersAccordingTo(result);
        assertThat(failedAllocations).isEmpty();
        assertThat(taskManagerTracker.getPendingTaskManagers())
                .containsExactlyInAnyOrder(pendingTaskManagerJob1, pendingTaskManagerJob2);
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob1.getPendingTaskManagerId()))
                .containsEntry(
                        job1, ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE_PROFILE, 1));
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob2.getPendingTaskManagerId()))
                .containsEntry(
                        job2, ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE_PROFILE, 1));

        taskManagerTracker.clearAllPendingAllocations();
        assertThat(taskManagerTracker.getPendingTaskManagers()).isEmpty();
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob1.getPendingTaskManagerId()))
                .isEmpty();
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob2.getPendingTaskManagerId()))
                .isEmpty();
    }

    @Test
    void testClearPendingAllocationsForJob() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();

        JobID job1 = new JobID();
        JobID job2 = new JobID();
        PendingTaskManager pendingTaskManagerJob1 =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        PendingTaskManager pendingTaskManagerJob2 =
                new PendingTaskManager(DEFAULT_TOTAL_RESOURCE_PROFILE, 1);
        ResourceAllocationResult result =
                new ResourceAllocationResult.Builder()
                        .addPendingTaskManagerAllocate(pendingTaskManagerJob1)
                        .addAllocationOnPendingResource(
                                job1,
                                pendingTaskManagerJob1.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .addPendingTaskManagerAllocate(pendingTaskManagerJob2)
                        .addAllocationOnPendingResource(
                                job2,
                                pendingTaskManagerJob2.getPendingTaskManagerId(),
                                DEFAULT_SLOT_RESOURCE_PROFILE)
                        .build();

        Set<PendingTaskManagerId> failedAllocations =
                taskManagerTracker.allocateTaskManagersAccordingTo(result);
        assertThat(failedAllocations).isEmpty();
        assertThat(taskManagerTracker.getPendingTaskManagers())
                .containsExactlyInAnyOrder(pendingTaskManagerJob1, pendingTaskManagerJob2);
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob1.getPendingTaskManagerId()))
                .containsEntry(
                        job1, ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE_PROFILE, 1));
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob2.getPendingTaskManagerId()))
                .containsEntry(
                        job2, ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE_PROFILE, 1));

        taskManagerTracker.clearPendingAllocationsOfJob(job1);
        assertThat(taskManagerTracker.getPendingTaskManagers())
                .containsExactly(pendingTaskManagerJob2);
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob1.getPendingTaskManagerId()))
                .isEmpty();
        assertThat(
                        taskManagerTracker.getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManagerJob2.getPendingTaskManagerId()))
                .containsEntry(
                        job2, ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE_PROFILE, 1));
    }

    @Test
    void testGetStatistics() {
        final FineGrainedTaskManagerTracker taskManagerTracker = createAndStartTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final ResourceProfile defaultSlotResource = ResourceProfile.fromResources(1, 100);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.registerTaskManager(
                TASK_EXECUTOR_CONNECTION, totalResource, defaultSlotResource, null);
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                defaultSlotResource,
                SlotState.ALLOCATED);

        PendingTaskManager pendingTaskManager =
                new PendingTaskManager(ResourceProfile.fromResources(4, 200), 1);
        taskManagerTracker.allocateTaskManagersAccordingTo(
                new ResourceAllocationResult.Builder()
                        .addPendingTaskManagerAllocate(pendingTaskManager)
                        .addAllocationOnPendingResource(
                                jobId,
                                pendingTaskManager.getPendingTaskManagerId(),
                                ResourceProfile.fromResources(4, 200))
                        .build());

        assertThat(taskManagerTracker.getFreeResource())
                .isEqualTo(ResourceProfile.fromResources(6, 700));
        assertThat(taskManagerTracker.getRegisteredResource()).isEqualTo(totalResource);
        assertThat(taskManagerTracker.getNumberRegisteredSlots()).isEqualTo(10);
        assertThat(taskManagerTracker.getNumberFreeSlots()).isEqualTo(8);
        assertThat(taskManagerTracker.getPendingResource())
                .isEqualTo(ResourceProfile.fromResources(4, 200));
    }

    @Test
    void testTimeoutForUnusedTaskManager() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(50L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();

        ResourceAllocator resourceAllocator =
                new TestingResourceAllocatorBuilder()
                        .setDeclareResourceNeededConsumer(
                                (resourceDeclarations) -> {
                                    assertThat(resourceDeclarations.size()).isEqualTo(1);
                                    ResourceDeclaration resourceDeclaration =
                                            resourceDeclarations.iterator().next();
                                    assertThat(resourceDeclaration.getNumNeeded()).isEqualTo(0);
                                    assertThat(resourceDeclaration.getUnwantedWorkers().size())
                                            .isEqualTo(1);

                                    releaseResourceFuture.complete(
                                            resourceDeclaration
                                                    .getUnwantedWorkers()
                                                    .iterator()
                                                    .next());
                                })
                        .build();

        FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTrackerBuilder(
                                new ScheduledExecutorServiceAdapter(
                                        EXECUTOR_RESOURCE.getExecutor()))
                        .setTaskManagerTimeout(taskManagerTimeout)
                        .build();
        taskManagerTracker.initialize(resourceAllocator, EXECUTOR_RESOURCE.getExecutor());

        taskManagerTracker.registerTaskManager(
                TASK_EXECUTOR_CONNECTION,
                DEFAULT_TOTAL_RESOURCE_PROFILE,
                DEFAULT_SLOT_RESOURCE_PROFILE,
                null);

        AllocationID allocationID = new AllocationID();
        JobID jobID = new JobID();
        taskManagerTracker.notifySlotStatus(
                allocationID,
                jobID,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);

        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getIdleSince())
                                        .isEqualTo(Long.MAX_VALUE));

        taskManagerTracker.notifySlotStatus(
                allocationID,
                jobID,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.FREE);

        assertThat(
                        taskManagerTracker.getRegisteredTaskManager(
                                TASK_EXECUTOR_CONNECTION.getInstanceID()))
                .hasValueSatisfying(
                        taskManagerInfo ->
                                assertThat(taskManagerInfo.getIdleSince())
                                        .isNotEqualTo(Long.MAX_VALUE));

        assertThatFuture(releaseResourceFuture)
                .eventuallySucceeds()
                .isEqualTo(TASK_EXECUTOR_CONNECTION.getInstanceID());
    }

    private FineGrainedTaskManagerTracker createTaskManagerTracker() {
        return new FineGrainedTaskManagerTrackerBuilder(
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))
                .build();
    }

    private FineGrainedTaskManagerTracker createAndStartTaskManagerTracker() {
        FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTrackerBuilder(
                                new ScheduledExecutorServiceAdapter(
                                        EXECUTOR_RESOURCE.getExecutor()))
                        .build();
        taskManagerTracker.initialize(
                new TestingResourceAllocatorBuilder().build(), EXECUTOR_RESOURCE.getExecutor());
        return taskManagerTracker;
    }
}
