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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link FineGrainedTaskManagerTracker}. */
public class FineGrainedTaskManagerTrackerTest extends TestLogger {
    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    @Test
    public void testInitState() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        assertThat(taskManagerTracker.getPendingTaskManagers(), is(empty()));
        assertThat(taskManagerTracker.getRegisteredTaskManagers(), is(empty()));
    }

    @Test
    public void testAddAndRemoveTaskManager() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();

        // Add task manager
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, ResourceProfile.ANY, ResourceProfile.ANY);
        assertThat(taskManagerTracker.getRegisteredTaskManagers().size(), is(1));
        assertTrue(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .isPresent());

        // Remove task manager
        taskManagerTracker.removeTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID());
        assertThat(taskManagerTracker.getRegisteredTaskManagers().size(), is(0));
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveUnknownTaskManager() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();

        taskManagerTracker.removeTaskManager(new InstanceID());
    }

    @Test
    public void testAddAndRemovePendingTaskManager() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final JobID jobId = new JobID();
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(ResourceProfile.ANY, 1);

        // Add pending task manager
        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        assertThat(taskManagerTracker.getPendingTaskManagers().size(), is(1));
        assertThat(
                taskManagerTracker
                        .getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                                ResourceProfile.ANY, ResourceProfile.ANY)
                        .size(),
                is(1));

        // Remove pending task manager
        final Map<JobID, ResourceCounter> records =
                taskManagerTracker.removePendingTaskManager(
                        pendingTaskManager.getPendingTaskManagerId());
        assertThat(taskManagerTracker.getPendingTaskManagers(), is(empty()));
        assertThat(
                taskManagerTracker
                        .getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManager.getPendingTaskManagerId())
                        .size(),
                is(0));
        assertThat(
                taskManagerTracker
                        .getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                                ResourceProfile.ANY, ResourceProfile.ANY)
                        .size(),
                is(0));
        assertTrue(records.containsKey(jobId));
        assertThat(records.get(jobId).getResourceCount(ResourceProfile.ANY), is(1));
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveUnknownPendingTaskManager() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();

        taskManagerTracker.removePendingTaskManager(PendingTaskManagerId.generate());
    }

    @Test
    public void testSlotAllocation() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
        // Notify free slot is now pending
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.PENDING);
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).isPresent());
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAvailableResource(),
                is(ResourceProfile.fromResources(7, 800)));

        // Notify pending slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId1,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(3, 200),
                SlotState.ALLOCATED);
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).isPresent());
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAvailableResource(),
                is(ResourceProfile.fromResources(7, 800)));

        // Notify free slot is now allocated
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.ALLOCATED);
        assertTrue(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2).isPresent());
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAvailableResource(),
                is(ResourceProfile.fromResources(5, 500)));
    }

    @Test
    public void testFreeSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(TASK_EXECUTOR_CONNECTION, totalResource, totalResource);
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
        assertFalse(taskManagerTracker.getAllocatedOrPendingSlot(allocationId1).isPresent());
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAvailableResource(),
                is(ResourceProfile.fromResources(8, 700)));
        // Free allocated slot
        taskManagerTracker.notifySlotStatus(
                allocationId2,
                jobId,
                TASK_EXECUTOR_CONNECTION.getInstanceID(),
                ResourceProfile.fromResources(2, 300),
                SlotState.FREE);
        assertFalse(taskManagerTracker.getAllocatedOrPendingSlot(allocationId2).isPresent());
        assertThat(
                taskManagerTracker
                        .getRegisteredTaskManager(TASK_EXECUTOR_CONNECTION.getInstanceID())
                        .get()
                        .getAvailableResource(),
                is(totalResource));
    }

    @Test(expected = NullPointerException.class)
    public void testFreeUnknownSlot() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();

        taskManagerTracker.notifySlotStatus(
                new AllocationID(),
                new JobID(),
                new InstanceID(),
                ResourceProfile.ANY,
                SlotState.FREE);
    }

    @Test
    public void testRecordPendingAllocations() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final PendingTaskManager pendingTaskManager1 =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final PendingTaskManager pendingTaskManager2 =
                new PendingTaskManager(ResourceProfile.ANY, 1);
        final JobID jobId = new JobID();
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(ResourceProfile.ANY, 1);
        taskManagerTracker.addPendingTaskManager(pendingTaskManager1);
        taskManagerTracker.addPendingTaskManager(pendingTaskManager2);

        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager1.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        // Only the last time is recorded
        taskManagerTracker.replaceAllPendingAllocations(
                Collections.singletonMap(
                        pendingTaskManager2.getPendingTaskManagerId(),
                        Collections.singletonMap(jobId, resourceCounter)));
        assertThat(
                taskManagerTracker
                        .getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManager1.getPendingTaskManagerId())
                        .size(),
                is(0));
        assertTrue(
                taskManagerTracker
                        .getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManager2.getPendingTaskManagerId())
                        .containsKey(jobId));
        assertThat(
                taskManagerTracker
                        .getPendingAllocationsOfPendingTaskManager(
                                pendingTaskManager2.getPendingTaskManagerId())
                        .get(jobId)
                        .getResourceCount(ResourceProfile.ANY),
                is(1));
    }

    @Test
    public void testGetStatistics() {
        final FineGrainedTaskManagerTracker taskManagerTracker =
                new FineGrainedTaskManagerTracker();
        final ResourceProfile totalResource = ResourceProfile.fromResources(10, 1000);
        final ResourceProfile defaultSlotResource = ResourceProfile.fromResources(1, 100);
        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();
        final JobID jobId = new JobID();
        taskManagerTracker.addTaskManager(
                TASK_EXECUTOR_CONNECTION, totalResource, defaultSlotResource);
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
        taskManagerTracker.addPendingTaskManager(
                new PendingTaskManager(ResourceProfile.fromResources(4, 200), 1));

        assertThat(taskManagerTracker.getFreeResource(), is(ResourceProfile.fromResources(6, 700)));
        assertThat(taskManagerTracker.getRegisteredResource(), is(totalResource));
        assertThat(taskManagerTracker.getNumberRegisteredSlots(), is(10));
        assertThat(taskManagerTracker.getNumberFreeSlots(), is(8));
        assertThat(
                taskManagerTracker.getPendingResource(), is(ResourceProfile.fromResources(4, 200)));
    }
}
