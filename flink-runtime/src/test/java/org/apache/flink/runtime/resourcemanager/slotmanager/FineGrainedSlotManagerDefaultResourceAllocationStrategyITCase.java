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
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT Cases of {@link FineGrainedSlotManager}, with the {@link DefaultResourceAllocationStrategy}.
 */
class FineGrainedSlotManagerDefaultResourceAllocationStrategyITCase
        extends AbstractFineGrainedSlotManagerITCase {
    private static final ResourceProfile OTHER_SLOT_RESOURCE_PROFILE =
            DEFAULT_TOTAL_RESOURCE_PROFILE.multiply(2);

    @Override
    protected Optional<ResourceAllocationStrategy> getResourceAllocationStrategy(
            SlotManagerConfiguration slotManagerConfiguration) {
        return Optional.of(
                new DefaultResourceAllocationStrategy(
                        DEFAULT_TOTAL_RESOURCE_PROFILE,
                        DEFAULT_NUM_SLOTS_PER_WORKER,
                        slotManagerConfiguration.isEvenlySpreadOutSlots(),
                        slotManagerConfiguration.getTaskManagerTimeout(),
                        slotManagerConfiguration.getRedundantTaskManagerNum(),
                        slotManagerConfiguration.getMinTotalCpu(),
                        slotManagerConfiguration.getMinTotalMem()));
    }

    /**
     * Test that the slot manager only allocates new workers if their worker spec can fulfill the
     * requested resource profile.
     */
    @Test
    void testWorkerOnlyAllocatedIfRequestedSlotCouldBeFulfilled() throws Exception {
        final AtomicInteger declareResourceCount = new AtomicInteger(0);

        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (ignored) -> declareResourceCount.incrementAndGet());
                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            createResourceRequirements(
                                                                    new JobID(),
                                                                    1,
                                                                    OTHER_SLOT_RESOURCE_PROFILE)));
                            assertThat(declareResourceCount.get()).isEqualTo(0);
                        });
            }
        };
    }

    /**
     * Tests that un-registration of task managers will cause resource missing again in
     * ResourceTracker.
     */
    @Test
    void testTaskManagerUnregisterAfterResourceRequirements() throws Exception {
        final TaskExecutorGateway taskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setRequestSlotFunction(tuple6 -> new CompletableFuture<>())
                        .createTestingTaskExecutorGateway();
        final ResourceID resourceId = ResourceID.generate();
        final TaskExecutorConnection taskManagerConnection =
                new TaskExecutorConnection(resourceId, taskExecutorGateway);
        final SlotReport slotReport =
                new SlotReport(
                        new SlotStatus(new SlotID(resourceId, 0), DEFAULT_SLOT_RESOURCE_PROFILE));
        new Context() {
            {
                slotManagerConfigurationBuilder.setRequirementCheckDelay(Duration.ZERO);
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
                            assertThat(getTaskManagerTracker().getNumberFreeSlots()).isEqualTo(2);

                            ResourceRequirements resourceRequirements =
                                    createResourceRequirementsForSingleSlot();

                            runInMainThreadAndWait(
                                    () ->
                                            getSlotManager()
                                                    .processResourceRequirements(
                                                            resourceRequirements));

                            assertThat(getTaskManagerTracker().getFreeResource())
                                    .isEqualTo(DEFAULT_SLOT_RESOURCE_PROFILE);
                            assertThat(getResourceTracker().getMissingResources()).isEmpty();

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
                            assertThat(getResourceTracker().getMissingResources())
                                    .containsKey(resourceRequirements.getJobId());
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
    void testTimeoutForUnusedTaskManager() throws Exception {
        final Time taskManagerTimeout = Time.milliseconds(50L);

        final CompletableFuture<InstanceID> releaseResourceFuture = new CompletableFuture<>();
        final AllocationID allocationId = new AllocationID();
        final TaskExecutorConnection taskExecutionConnection = createTaskExecutorConnection();
        final InstanceID instanceId = taskExecutionConnection.getInstanceID();
        new Context() {
            {
                resourceAllocatorBuilder.setDeclareResourceNeededConsumer(
                        (resourceDeclarations) -> {
                            assertThat(resourceDeclarations).hasSize(1);
                            ResourceDeclaration resourceDeclaration =
                                    resourceDeclarations.iterator().next();
                            assertThat(resourceDeclaration.getNumNeeded()).isEqualTo(0);
                            assertThat(resourceDeclaration.getUnwantedWorkers()).hasSize(1);
                            releaseResourceFuture.complete(
                                    resourceDeclaration.getUnwantedWorkers().iterator().next());
                        });
                slotManagerConfigurationBuilder.setTaskManagerTimeout(taskManagerTimeout);
                runTest(
                        () -> {
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
                                                                                    new JobID(),
                                                                                    allocationId,
                                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)),
                                                                    DEFAULT_TOTAL_RESOURCE_PROFILE,
                                                                    DEFAULT_SLOT_RESOURCE_PROFILE)));
                            assertThat(assertFutureCompleteAndReturn(registerTaskManagerFuture))
                                    .isEqualTo(SlotManager.RegistrationResult.SUCCESS);
                            assertThat(getSlotManager().getTaskManagerIdleSince(instanceId))
                                    .isEqualTo(Long.MAX_VALUE);

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

                            assertThat(assertFutureCompleteAndReturn(idleSinceFuture))
                                    .isNotEqualTo(Long.MAX_VALUE);
                            assertThat(assertFutureCompleteAndReturn(releaseResourceFuture))
                                    .isEqualTo(instanceId);
                            // A task manager timeout does not remove the slots from the
                            // SlotManager. The receiver of the callback can then decide what to do
                            // with the TaskManager.
                            assertThat(getSlotManager().getNumberRegisteredSlots())
                                    .isEqualTo(DEFAULT_NUM_SLOTS_PER_WORKER);

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
                            assertThat(assertFutureCompleteAndReturn(unregisterTaskManagerFuture))
                                    .isTrue();
                            assertThat(getSlotManager().getNumberRegisteredSlots()).isEqualTo(0);
                        });
            }
        };
    }
}
