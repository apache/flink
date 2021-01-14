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
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executor;

/** Base class for the tests of {@link FineGrainedSlotManager}. */
public abstract class FineGrainedSlotManagerTestBase extends TestLogger {
    private static final Executor MAIN_THREAD_EXECUTOR = Executors.directExecutor();
    static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");
    static final long FUTURE_TIMEOUT_SECOND = 5;

    /** Resource profile for the default task manager. */
    protected abstract ResourceProfile getDefaultTaskManagerResourceProfile();

    /** Resource profile for the default slot and requirement. */
    protected abstract ResourceProfile getDefaultSlotResourceProfile();

    /** The number of slot for the default task manager. */
    protected abstract int getDefaultNumberSlotsPerWorker();

    /**
     * Resource profile for a larger task manager, which can fulfill both the larger and the default
     * slots.
     */
    protected abstract ResourceProfile getLargeTaskManagerResourceProfile();

    /**
     * Resource profile for a larger slot or requirement, which can be fulfilled by the task manager
     * and cannot be fulfilled by the default task manager.
     */
    protected abstract ResourceProfile getLargeSlotResourceProfile();

    protected abstract Optional<ResourceAllocationStrategy> getResourceAllocationStrategy();

    static SlotStatus createAllocatedSlotStatus(
            AllocationID allocationID, ResourceProfile resourceProfile) {
        return new SlotStatus(
                new SlotID(ResourceID.generate(), 0), resourceProfile, new JobID(), allocationID);
    }

    static int getTotalResourceCount(Collection<ResourceRequirement> resources) {
        if (resources == null) {
            return 0;
        }
        return resources.stream()
                .map(ResourceRequirement::getNumberOfRequiredSlots)
                .reduce(0, Integer::sum);
    }

    static ResourceRequirements createResourceRequirementsForSingleSlot() {
        return createResourceRequirementsForSingleSlot(new JobID());
    }

    static ResourceRequirements createResourceRequirementsForSingleSlot(JobID jobId) {
        return createResourceRequirements(jobId, 1);
    }

    static ResourceRequirements createResourceRequirements(JobID jobId, int numRequiredSlots) {
        return createResourceRequirements(jobId, numRequiredSlots, ResourceProfile.UNKNOWN);
    }

    static ResourceRequirements createResourceRequirements(
            JobID jobId, int numRequiredSlots, ResourceProfile resourceProfile) {
        return ResourceRequirements.create(
                jobId,
                "foobar",
                Collections.singleton(
                        ResourceRequirement.create(resourceProfile, numRequiredSlots)));
    }

    static TaskExecutorConnection createTaskExecutorConnection() {
        return new TaskExecutorConnection(
                ResourceID.generate(),
                new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());
    }

    /** This class provides a self-contained context for each test case. */
    protected class Context {
        private final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
        private final ResourceTracker resourceTracker = new DefaultResourceTracker();
        private final TaskManagerTracker taskManagerTracker = new FineGrainedTaskManagerTracker();
        private final SlotStatusSyncer slotStatusSyncer =
                new DefaultSlotStatusSyncer(Time.seconds(10L));
        private final ScheduledExecutor scheduledExecutor = TestingUtils.defaultScheduledExecutor();
        private final SlotManagerMetricGroup slotManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
        private final Executor mainThreadExecutor = MAIN_THREAD_EXECUTOR;
        private FineGrainedSlotManager slotManager;

        final TestingResourceAllocationStrategy.Builder resourceAllocationStrategyBuilder =
                TestingResourceAllocationStrategy.newBuilder();

        final TestingResourceActionsBuilder resourceActionsBuilder =
                new TestingResourceActionsBuilder();
        final SlotManagerConfigurationBuilder slotManagerConfigurationBuilder =
                SlotManagerConfigurationBuilder.newBuilder();

        FineGrainedSlotManager getSlotManager() {
            return slotManager;
        }

        ResourceTracker getResourceTracker() {
            return resourceTracker;
        }

        TaskManagerTracker getTaskManagerTracker() {
            return taskManagerTracker;
        }

        ResourceManagerId getResourceManagerId() {
            return resourceManagerId;
        }

        protected final void runTest(RunnableWithException testMethod) throws Exception {
            slotManager =
                    new FineGrainedSlotManager(
                            scheduledExecutor,
                            slotManagerConfigurationBuilder.build(),
                            slotManagerMetricGroup,
                            resourceTracker,
                            taskManagerTracker,
                            slotStatusSyncer,
                            getResourceAllocationStrategy()
                                    .orElse(resourceAllocationStrategyBuilder.build()));
            slotManager.start(
                    resourceManagerId, mainThreadExecutor, resourceActionsBuilder.build());

            testMethod.run();

            slotManager.close();
        }
    }
}
