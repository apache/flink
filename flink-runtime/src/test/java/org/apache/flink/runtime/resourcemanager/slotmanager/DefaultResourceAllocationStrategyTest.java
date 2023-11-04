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
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultResourceAllocationStrategy}. */
class DefaultResourceAllocationStrategyTest {
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE =
            ResourceProfile.fromResources(1, 100);
    private static final int NUM_OF_SLOTS = 5;
    private static final DefaultResourceAllocationStrategy ANY_MATCHING_STRATEGY =
            createStrategy(false);

    private static final DefaultResourceAllocationStrategy EVENLY_STRATEGY = createStrategy(true);

    @Test
    void testFulfillRequirementWithRegisteredResources() {
        final TaskManagerInfo taskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(8);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(() -> Collections.singleton(taskManager))
                        .build();
        requirements.add(ResourceRequirement.create(largeResource, 1));
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
        assertThat(
                        result.getAllocationsOnRegisteredResources()
                                .get(jobId)
                                .get(taskManager.getInstanceId())
                                .getResourceCount(DEFAULT_SLOT_RESOURCE))
                .isEqualTo(2);
        assertThat(
                        result.getAllocationsOnRegisteredResources()
                                .get(jobId)
                                .get(taskManager.getInstanceId())
                                .getResourceCount(largeResource))
                .isEqualTo(1);
    }

    @Test
    void testFulfillRequirementWithRegisteredResourcesEvenly() {
        final TaskManagerInfo taskManager1 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerInfo taskManager2 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerInfo taskManager3 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);

        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(5);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManager1, taskManager2, taskManager3))
                        .build();
        requirements.add(ResourceRequirement.create(largeResource, 4));
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));

        final ResourceAllocationResult result =
                EVENLY_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();

        assertThat(result.getAllocationsOnRegisteredResources().get(jobId).values())
                .allSatisfy(
                        resourceCounter ->
                                assertThat(resourceCounter.getTotalResourceCount()).isEqualTo(2));
        assertThat(result.getAllocationsOnRegisteredResources().get(jobId).values())
                .allSatisfy(
                        resourceCounter ->
                                assertThat(resourceCounter.containsResource(largeResource))
                                        .isTrue());
    }

    @Test
    void testExcessPendingResourcesCouldReleaseEvenly() {
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () ->
                                        Arrays.asList(
                                                new PendingTaskManager(
                                                        DEFAULT_SLOT_RESOURCE.multiply(2), 2),
                                                new PendingTaskManager(
                                                        DEFAULT_SLOT_RESOURCE.multiply(2), 2)))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));

        final ResourceAllocationResult result =
                EVENLY_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);

        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).hasSize(1);
    }

    @Test
    void testSpecialResourcesRequirementCouldFulfilledEvenly() {
        testSpecialResourcesRequirementCouldFulfilled(EVENLY_STRATEGY);
    }

    @Test
    void testSpecialResourcesRequirementCouldFulfilledAnyMatching() {
        testSpecialResourcesRequirementCouldFulfilled(ANY_MATCHING_STRATEGY);
    }

    void testSpecialResourcesRequirementCouldFulfilled(DefaultResourceAllocationStrategy strategy) {
        ResourceProfile extendedResourceProfile =
                ResourceProfile.newBuilder(DEFAULT_SLOT_RESOURCE)
                        .setExtendedResource(new ExternalResource("customResource", 1))
                        .build();

        final TaskManagerInfo extendedTaskManager =
                new TestingTaskManagerInfo(
                        extendedResourceProfile.multiply(2),
                        extendedResourceProfile.multiply(1),
                        extendedResourceProfile);

        final TaskManagerInfo defaultTaskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(2),
                        DEFAULT_SLOT_RESOURCE.multiply(2),
                        DEFAULT_SLOT_RESOURCE);

        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(defaultTaskManager, extendedTaskManager))
                        .build();

        requirements.add(ResourceRequirement.create(extendedResourceProfile, 1));

        final ResourceAllocationResult result =
                strategy.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);

        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
        assertThat(result.getAllocationsOnRegisteredResources()).hasSize(1);
        assertThat(result.getAllocationsOnRegisteredResources().get(jobId).keySet())
                .satisfiesExactly(
                        instanceId ->
                                assertThat(instanceId)
                                        .isEqualTo(extendedTaskManager.getInstanceId()));
    }

    @Test
    void testFulfillRequirementWithPendingResources() {
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(3);
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();
        requirements.add(ResourceRequirement.create(largeResource, 2));
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 4));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnRegisteredResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(1);
        final PendingTaskManagerId newAllocated =
                result.getPendingTaskManagersToAllocate().get(0).getPendingTaskManagerId();
        ResourceCounter allFulfilledRequirements = ResourceCounter.empty();
        for (Map.Entry<ResourceProfile, Integer> resourceWithCount :
                result.getAllocationsOnPendingResources()
                        .get(pendingTaskManager.getPendingTaskManagerId())
                        .get(jobId)
                        .getResourcesWithCount()) {
            allFulfilledRequirements =
                    allFulfilledRequirements.add(
                            resourceWithCount.getKey(), resourceWithCount.getValue());
        }
        for (Map.Entry<ResourceProfile, Integer> resourceWithCount :
                result.getAllocationsOnPendingResources()
                        .get(newAllocated)
                        .get(jobId)
                        .getResourcesWithCount()) {
            allFulfilledRequirements =
                    allFulfilledRequirements.add(
                            resourceWithCount.getKey(), resourceWithCount.getValue());
        }

        assertThat(allFulfilledRequirements.getResourceCount(DEFAULT_SLOT_RESOURCE)).isEqualTo(4);
        assertThat(allFulfilledRequirements.getResourceCount(largeResource)).isEqualTo(2);
    }

    @Test
    void testUnfulfillableRequirement() {
        final TaskManagerInfo taskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final ResourceProfile unfulfillableResource = DEFAULT_SLOT_RESOURCE.multiply(8);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(() -> Collections.singleton(taskManager))
                        .build();
        requirements.add(ResourceRequirement.create(unfulfillableResource, 1));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).containsExactly(jobId);
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
    }

    /** Tests that blocked task manager cannot fulfill requirements. */
    @Test
    void testBlockedTaskManagerCannotFulfillRequirements() {
        final TaskManagerInfo registeredTaskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Collections.singleton(registeredTaskManager))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2 * NUM_OF_SLOTS));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        registeredTaskManager.getTaskExecutorConnection().getResourceID()::equals);

        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnRegisteredResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(2);
    }

    @Test
    void testIdleTaskManagerShouldBeReleased() {
        final TestingTaskManagerInfo registeredTaskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Collections.singleton(registeredTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getTaskManagersToRelease()).isEmpty();

        registeredTaskManager.setIdleSince(System.currentTimeMillis() - 10);

        result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);
        assertThat(result.getTaskManagersToRelease()).containsExactly(registeredTaskManager);
    }

    @Test
    void testIdlePendingTaskManagerShouldBeReleased() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE, 1);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).containsExactly(pendingTaskManager);
    }

    @Test
    void testUsedPendingTaskManagerShouldNotBeReleased() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE, 1);
        pendingTaskManager.replaceAllPendingAllocations(
                Collections.singletonMap(
                        new JobID(), ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE, 1)));
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
    }

    @Test
    void testFulFillRequirementShouldTakeRedundantInAccount() {
        DefaultResourceAllocationStrategy strategy = createStrategy(1);

        final TaskManagerInfo taskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE);

        final JobID jobId = new JobID();
        final ResourceProfile largeResource = DEFAULT_SLOT_RESOURCE.multiply(4);
        final List<ResourceRequirement> requirements =
                Collections.singletonList(ResourceRequirement.create(largeResource, 1));

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(() -> Collections.singleton(taskManager))
                        .build();

        final ResourceAllocationResult result =
                strategy.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(1);
        assertThat(result.getAllocationsOnPendingResources()).isEmpty();
    }

    @Test
    void testUnusedResourcesShouldBeReleasedIfNonIdleResourceIsEnough() {
        final TaskManagerInfo taskManagerInUse =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(2),
                        DEFAULT_SLOT_RESOURCE);

        final TestingTaskManagerInfo taskManagerIdle =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE);
        taskManagerIdle.setIdleSince(System.currentTimeMillis() - 10);

        final PendingTaskManager pendingTaskManagerInUse =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(5), NUM_OF_SLOTS);
        pendingTaskManagerInUse.replaceAllPendingAllocations(
                Collections.singletonMap(
                        new JobID(), ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE, 2)));

        final PendingTaskManager pendingTaskManagerIdle =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(5), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManagerInUse, taskManagerIdle))
                        .setPendingTaskManagersSupplier(
                                () ->
                                        Arrays.asList(
                                                pendingTaskManagerInUse, pendingTaskManagerIdle))
                        .build();

        DefaultResourceAllocationStrategy strategy = createStrategy(1);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);
        assertThat(result.getPendingTaskManagersToRelease())
                .containsExactly(pendingTaskManagerIdle);
        assertThat(result.getTaskManagersToRelease()).containsExactly(taskManagerIdle);
    }

    @Test
    void testRedundantResourceShouldBeFulfilled() {
        final TaskManagerInfo taskManagerInUse =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(2),
                        DEFAULT_SLOT_RESOURCE);

        final TestingTaskManagerInfo taskManagerIdle =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE);
        taskManagerIdle.setIdleSince(System.currentTimeMillis() - 10);

        final PendingTaskManager pendingTaskManagerIdle =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(5), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManagerInUse, taskManagerIdle))
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManagerIdle))
                        .build();

        DefaultResourceAllocationStrategy strategy = createStrategy(4);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        // pending task manager should reserved for redundant
        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
        // both in use and idle task manager should be reserved for redundant
        assertThat(result.getTaskManagersToRelease()).isEmpty();
        // add two more pending task manager for redundant since total available resource equals
        // 12(2+5+5)
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(2);
    }

    @Test
    void testRedundantResourceShouldBeReserved() {
        final TaskManagerInfo taskManagerInUse =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(2),
                        DEFAULT_SLOT_RESOURCE);

        final TestingTaskManagerInfo taskManagerIdle =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE.multiply(5),
                        DEFAULT_SLOT_RESOURCE);
        taskManagerIdle.setIdleSince(System.currentTimeMillis() - 10);

        final PendingTaskManager pendingTaskManagerIdle =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(5), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManagerInUse, taskManagerIdle))
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManagerIdle))
                        .build();

        DefaultResourceAllocationStrategy strategy = createStrategy(1);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);
        // pending task manager should release at first
        assertThat(result.getPendingTaskManagersToRelease())
                .containsExactly(pendingTaskManagerIdle);
        // idle task manager should reserved for redundant
        assertThat(result.getTaskManagersToRelease()).isEmpty();
    }

    @Test
    void testMinRequiredCPULimitInTryReconcile() {
        CPUResource minRequiredCPU =
                DEFAULT_SLOT_RESOURCE
                        .getCpuCores()
                        .multiply(NUM_OF_SLOTS)
                        .multiply(BigDecimal.valueOf(4.5));

        testMinResourceLimitInReconcile(minRequiredCPU, MemorySize.ZERO, 2);
        testMinResourceLimitInReconcileWithNoResource(minRequiredCPU, MemorySize.ZERO, 5);
    }

    @Test
    void testMinRequiredMemoryLimitInTryReconcile() {
        MemorySize minRequiredMemory =
                DEFAULT_SLOT_RESOURCE.getTotalMemory().multiply(NUM_OF_SLOTS).multiply(3.5);
        testMinResourceLimitInReconcile(new CPUResource(0.0), minRequiredMemory, 1);
        testMinResourceLimitInReconcileWithNoResource(new CPUResource(0.0), minRequiredMemory, 4);
    }

    @Test
    void testMinRequiredCPULimitInTryFulfill() {
        CPUResource minRequiredCPU =
                DEFAULT_SLOT_RESOURCE
                        .getCpuCores()
                        .multiply(NUM_OF_SLOTS)
                        .multiply(BigDecimal.valueOf(4.5));
        testMinRequiredResourceLimitInFulfillRequirements(minRequiredCPU, MemorySize.ZERO, 4);
    }

    @Test
    void testMinRequiredCPULimitInTryFulfillWithRequirement() {
        CPUResource minRequiredCPU =
                DEFAULT_SLOT_RESOURCE
                        .getCpuCores()
                        .multiply(NUM_OF_SLOTS)
                        .multiply(BigDecimal.valueOf(4.5));
        List<ResourceRequirement> resourceRequirements =
                Collections.singletonList(
                        ResourceRequirement.create(DEFAULT_SLOT_RESOURCE, 3 * NUM_OF_SLOTS));

        testMinRequiredResourceLimitInFulfillRequirements(
                minRequiredCPU, MemorySize.ZERO, 4, resourceRequirements);
    }

    @Test
    void testMinRequiredMemoryLimitInTryFulfill() {
        MemorySize minRequiredMemory =
                DEFAULT_SLOT_RESOURCE.getTotalMemory().multiply(NUM_OF_SLOTS).multiply(3.5);
        testMinRequiredResourceLimitInFulfillRequirements(
                new CPUResource(0.0), minRequiredMemory, 3);
    }

    @Test
    void testMinRequiredMemoryLimitInTryFulfillWithRequirement() {
        CPUResource minRequiredCPU =
                DEFAULT_SLOT_RESOURCE
                        .getCpuCores()
                        .multiply(NUM_OF_SLOTS)
                        .multiply(BigDecimal.valueOf(4.5));
        List<ResourceRequirement> resourceRequirements =
                Collections.singletonList(
                        ResourceRequirement.create(DEFAULT_SLOT_RESOURCE, 3 * NUM_OF_SLOTS));
        testMinRequiredResourceLimitInFulfillRequirements(
                minRequiredCPU, MemorySize.ZERO, 4, resourceRequirements);
    }

    void testMinResourceLimitInReconcile(
            CPUResource minRequiredCPU,
            MemorySize minRequiredMemory,
            int pendingTaskManagersToAllocate) {

        final TaskManagerInfo taskManagerInUse =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);

        final TestingTaskManagerInfo taskManagerIdle =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        taskManagerIdle.setIdleSince(System.currentTimeMillis() - 10);

        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManagerInUse, taskManagerIdle))
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManager))
                        .build();

        DefaultResourceAllocationStrategy strategy =
                createStrategy(minRequiredCPU, minRequiredMemory);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
        assertThat(result.getTaskManagersToRelease()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    void testMinResourceLimitInReconcileWithNoResource(
            CPUResource minRequiredCPU,
            MemorySize minRequiredMemory,
            int pendingTaskManagersToAllocate) {

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(Arrays::asList)
                        .setPendingTaskManagersSupplier(Arrays::asList)
                        .build();

        DefaultResourceAllocationStrategy strategy =
                createStrategy(minRequiredCPU, minRequiredMemory);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
        assertThat(result.getTaskManagersToRelease()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    void testMinRequiredResourceLimitInFulfillRequirements(
            CPUResource minRequiredCPU,
            MemorySize minRequiredMemory,
            int pendingTaskManagersToAllocate) {
        testMinRequiredResourceLimitInFulfillRequirements(
                minRequiredCPU,
                minRequiredMemory,
                pendingTaskManagersToAllocate,
                Collections.emptyList());
    }

    void testMinRequiredResourceLimitInFulfillRequirements(
            CPUResource minRequiredCPU,
            MemorySize minRequiredMemory,
            int pendingTaskManagersToAllocate,
            List<ResourceRequirement> requirements) {
        final JobID jobId = new JobID();
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(Arrays::asList)
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManager))
                        .build();

        DefaultResourceAllocationStrategy strategy =
                createStrategy(minRequiredCPU, minRequiredMemory);
        ResourceAllocationResult result =
                strategy.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);

        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    private static DefaultResourceAllocationStrategy createStrategy(boolean evenlySpreadOutSlots) {
        return createStrategy(evenlySpreadOutSlots, 0);
    }

    private static DefaultResourceAllocationStrategy createStrategy(int redundantTaskManagerNum) {
        return createStrategy(false, redundantTaskManagerNum);
    }

    private static DefaultResourceAllocationStrategy createStrategy(
            boolean evenlySpreadOutSlots, int redundantTaskManagerNum) {
        return new DefaultResourceAllocationStrategy(
                DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                NUM_OF_SLOTS,
                evenlySpreadOutSlots,
                Time.milliseconds(0),
                redundantTaskManagerNum,
                new CPUResource(0.0),
                MemorySize.ZERO);
    }

    private static DefaultResourceAllocationStrategy createStrategy(
            CPUResource minRequiredCPU, MemorySize minRequiredMemory) {
        return new DefaultResourceAllocationStrategy(
                DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                NUM_OF_SLOTS,
                false,
                Time.milliseconds(0),
                0,
                minRequiredCPU,
                minRequiredMemory);
    }
}
