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
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import akka.pattern.AskTimeoutException;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DefaultSlotStatusSyncer}. */
public class DefaultSlotStatusSyncerTest extends TestLogger {
    private static final Time TASK_MANAGER_REQUEST_TIMEOUT = Time.seconds(10);
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    public void testAllocateSlot() throws Exception {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final CompletableFuture<
                        Tuple6<
                                SlotID,
                                JobID,
                                AllocationID,
                                ResourceProfile,
                                String,
                                ResourceManagerId>>
                requestFuture = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> responseFuture = new CompletableFuture<>();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                tuple6 -> {
                                    requestFuture.complete(tuple6);
                                    return responseFuture;
                                })
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        taskManagerTracker.addTaskManager(
                taskExecutorConnection, ResourceProfile.ANY, ResourceProfile.ANY);
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                TestingUtils.defaultExecutor());

        final CompletableFuture<Void> allocatedFuture =
                slotStatusSyncer.allocateSlot(
                        taskExecutorConnection.getInstanceID(),
                        jobId,
                        "address",
                        ResourceProfile.ANY);
        final AllocationID allocationId = requestFuture.get().f2;
        assertThat(
                resourceTracker.getAcquiredResources(jobId),
                contains(ResourceRequirement.create(ResourceProfile.ANY, 1)));
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent());
        assertThat(
                taskManagerTracker.getAllocatedOrPendingSlot(allocationId).get().getJobId(),
                is(jobId));
        assertThat(
                taskManagerTracker.getAllocatedOrPendingSlot(allocationId).get().getState(),
                is(SlotState.PENDING));

        responseFuture.complete(Acknowledge.get());
        assertFalse(allocatedFuture.isCompletedExceptionally());
    }

    @Test
    public void testAllocateSlotFailsWithException() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(
                                ignored ->
                                        FutureUtils.completedExceptionally(
                                                new AskTimeoutException("timeout")))
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        taskManagerTracker.addTaskManager(
                taskExecutorConnection, ResourceProfile.ANY, ResourceProfile.ANY);
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                TestingUtils.defaultExecutor());

        final CompletableFuture<Void> allocatedFuture =
                slotStatusSyncer.allocateSlot(
                        taskExecutorConnection.getInstanceID(),
                        jobId,
                        "address",
                        ResourceProfile.ANY);
        try {
            allocatedFuture.get();
        } catch (Exception e) {
            assertThat(e.getCause(), instanceOf(AskTimeoutException.class));
        }
        assertThat(resourceTracker.getAcquiredResources(jobId), is(empty()));
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get()
                        .getAllocatedSlots()
                        .keySet(),
                is(empty()));
    }

    @Test
    public void testFreeSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final JobID jobId = new JobID();
        final AllocationID allocationId = new AllocationID();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                TestingUtils.defaultExecutor());
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, ResourceProfile.ANY, ResourceProfile.ANY);
        taskManagerTracker.notifySlotStatus(
                allocationId,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.ANY,
                SlotState.ALLOCATED);
        resourceTracker.notifyAcquiredResource(jobId, ResourceProfile.ANY);

        slotStatusSyncer.freeSlot(allocationId);
        assertThat(resourceTracker.getAcquiredResources(jobId), is(empty()));
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAllocatedSlots()
                        .keySet(),
                is(empty()));
    }

    @Test
    public void testSlotStatusProcessing() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceTracker resourceTracker = new DefaultResourceTracker();
        final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(TASK_MANAGER_REQUEST_TIMEOUT);
        slotStatusSyncer.initialize(
                taskManagerTracker,
                resourceTracker,
                ResourceManagerId.generate(),
                TestingUtils.defaultExecutor());
        final TestingTaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(ignored -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final TaskExecutorConnection taskExecutorConnection =
                new TaskExecutorConnection(ResourceID.generate(), taskExecutorGateway);
        final JobID jobId = new JobID();
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final SlotID slotId1 = new SlotID(taskExecutorConnection.getResourceID(), 0);
        final SlotID slotId2 = new SlotID(taskExecutorConnection.getResourceID(), 1);
        final SlotID slotId3 = new SlotID(taskExecutorConnection.getResourceID(), 2);
        final ResourceProfile totalResource = ResourceProfile.fromResources(5, 20);
        final ResourceProfile resource = ResourceProfile.fromResources(1, 4);
        final SlotReport slotReport1 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId1, totalResource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1),
                                new SlotStatus(slotId3, resource, jobId, allocationId2)));
        final SlotReport slotReport2 =
                new SlotReport(
                        Arrays.asList(
                                new SlotStatus(slotId3, resource),
                                new SlotStatus(slotId2, resource, jobId, allocationId1)));
        taskManagerTracker.addTaskManager(taskExecutorConnection, totalResource, totalResource);

        slotStatusSyncer.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport1);
        assertThat(
                resourceTracker.getAcquiredResources(jobId),
                contains(ResourceRequirement.create(resource, 2)));
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get()
                        .getAvailableResource(),
                equalTo(ResourceProfile.fromResources(3, 12)));
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).isPresent());
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2).isPresent());

        slotStatusSyncer.allocateSlot(
                taskExecutorConnection.getInstanceID(), jobId, "address", resource);
        assertThat(
                resourceTracker.getAcquiredResources(jobId),
                contains(ResourceRequirement.create(resource, 3)));
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get()
                        .getAvailableResource(),
                equalTo(ResourceProfile.fromResources(2, 8)));
        final AllocationID allocationId3 =
                taskManagerTracker.getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get().getAllocatedSlots().keySet().stream()
                        .filter(
                                allocationId ->
                                        !allocationId.equals(allocationId1)
                                                && !allocationId.equals(allocationId2))
                        .findAny()
                        .get();

        // allocationId1 should still be allocated; allocationId2 should be freed; allocationId3
        // should continue to be in a pending state;
        slotStatusSyncer.reportSlotStatus(taskExecutorConnection.getInstanceID(), slotReport2);
        assertThat(
                resourceTracker.getAcquiredResources(jobId),
                contains(ResourceRequirement.create(resource, 2)));
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                        .get()
                        .getAvailableResource(),
                equalTo(ResourceProfile.fromResources(3, 12)));
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).isPresent());
        assertFalse(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2).isPresent());
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId3).isPresent());
        assertThat(
                taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).get().getState(),
                is(SlotState.ALLOCATED));
        assertThat(
                taskManagerTracker.getAllocatedOrPendingSlot(allocationId3).get().getState(),
                is(SlotState.PENDING));
    }
}
