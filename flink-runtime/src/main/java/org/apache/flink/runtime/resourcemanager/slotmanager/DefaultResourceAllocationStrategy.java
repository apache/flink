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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerUtils.getEffectiveResourceProfile;

/**
 * The default implementation of {@link ResourceAllocationStrategy}.
 *
 * <p>For each requirement, this strategy tries to fulfill it with any registered or pending
 * resources (registered is prioritized). If a requirement cannot be fulfilled by any registered or
 * pending resources, it allocates a new pending resource, with the pre-defined total and default
 * slot resource profiles, thus all new pending resources should have the same profiles. A
 * requirement is considered unfulfillable if it is not fulfilled by any registered or pending
 * resources and cannot fit into the pre-defined total resource profile.
 *
 * <p>Note: This strategy tries to find a feasible allocation result, rather than an optimal one (in
 * term of resource utilization). It also does not guarantee always finding a feasible solution when
 * exist.
 *
 * <p>Note: The current implementation of this strategy is non-optimal, in terms of computation
 * efficiency. In the worst case, for each requirement it checks all registered and pending
 * resources. TODO: This will be optimized in FLINK-21174.
 */
public class DefaultResourceAllocationStrategy implements ResourceAllocationStrategy {
    private final ResourceProfile defaultSlotResourceProfile;
    private final ResourceProfile totalResourceProfile;
    private final int numSlotsPerWorker;

    public DefaultResourceAllocationStrategy(
            ResourceProfile totalResourceProfile, int numSlotsPerWorker) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        totalResourceProfile, numSlotsPerWorker);
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();

        // Tuples of available and default slot resource for registered task managers, indexed by
        // instanceId
        final Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources =
                getRegisteredResources(taskManagerResourceInfoProvider);
        // Available resources of pending task managers, indexed by the pendingTaskManagerId
        final Map<PendingTaskManagerId, ResourceProfile> pendingResources =
                getPendingResources(taskManagerResourceInfoProvider);

        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            final ResourceCounter unfulfilledJobRequirements =
                    tryFulfillRequirementsForJobWithRegisteredResources(
                            jobId,
                            resourceRequirements.getValue(),
                            registeredResources,
                            resultBuilder);

            if (!unfulfilledJobRequirements.isEmpty()) {
                tryFulfillRequirementsForJobWithPendingResources(
                        jobId, unfulfilledJobRequirements, pendingResources, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    private static Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> getRegisteredResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        return taskManagerResourceInfoProvider.getRegisteredTaskManagers().stream()
                .collect(
                        Collectors.toMap(
                                TaskManagerInfo::getInstanceId,
                                taskManager ->
                                        Tuple2.of(
                                                taskManager.getAvailableResource(),
                                                taskManager.getDefaultSlotResourceProfile())));
    }

    private static Map<PendingTaskManagerId, ResourceProfile> getPendingResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        return taskManagerResourceInfoProvider.getPendingTaskManagers().stream()
                .collect(
                        Collectors.toMap(
                                PendingTaskManager::getPendingTaskManagerId,
                                PendingTaskManager::getTotalResourceProfile));
    }

    private static ResourceCounter tryFulfillRequirementsForJobWithRegisteredResources(
            JobID jobId,
            Collection<ResourceRequirement> missingResources,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            ResourceAllocationResult.Builder resultBuilder) {
        ResourceCounter outstandingRequirements = ResourceCounter.empty();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingRequirements =
                    tryFindSlotsForRequirement(
                            jobId, resourceRequirement, registeredResources, resultBuilder);
            if (numMissingRequirements > 0) {
                outstandingRequirements =
                        outstandingRequirements.add(
                                resourceRequirement.getResourceProfile(), numMissingRequirements);
            }
        }
        return outstandingRequirements;
    }

    private static int tryFindSlotsForRequirement(
            JobID jobId,
            ResourceRequirement resourceRequirement,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            ResourceAllocationResult.Builder resultBuilder) {
        final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();

        int numUnfulfilled = resourceRequirement.getNumberOfRequiredSlots();
        while (numUnfulfilled > 0) {
            final Optional<InstanceID> matchedTaskManager =
                    findMatchingTaskManager(requiredResource, registeredResources);

            if (!matchedTaskManager.isPresent()) {
                // exit loop early; we won't find a matching slot for this requirement
                break;
            }

            final ResourceProfile effectiveProfile =
                    getEffectiveResourceProfile(
                            requiredResource, registeredResources.get(matchedTaskManager.get()).f1);
            resultBuilder.addAllocationOnRegisteredResource(
                    jobId, matchedTaskManager.get(), effectiveProfile);
            deductionRegisteredResource(
                    registeredResources, matchedTaskManager.get(), effectiveProfile);
            numUnfulfilled--;
        }
        return numUnfulfilled;
    }

    private static Optional<InstanceID> findMatchingTaskManager(
            ResourceProfile requirement,
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources) {
        return registeredResources.entrySet().stream()
                .filter(
                        taskManager ->
                                canFulfillRequirement(
                                        getEffectiveResourceProfile(
                                                requirement, taskManager.getValue().f1),
                                        taskManager.getValue().f0))
                .findAny()
                .map(Map.Entry::getKey);
    }

    private static boolean canFulfillRequirement(
            ResourceProfile requirement, ResourceProfile resourceProfile) {
        return resourceProfile.allFieldsNoLessThan(requirement);
    }

    private static void deductionRegisteredResource(
            Map<InstanceID, Tuple2<ResourceProfile, ResourceProfile>> registeredResources,
            InstanceID instanceId,
            ResourceProfile resourceProfile) {
        registeredResources.compute(
                instanceId,
                (id, tuple2) -> {
                    Preconditions.checkNotNull(tuple2);
                    if (tuple2.f0.subtract(resourceProfile).equals(ResourceProfile.ZERO)) {
                        return null;
                    } else {
                        return Tuple2.of(tuple2.f0.subtract(resourceProfile), tuple2.f1);
                    }
                });
    }

    private static Optional<PendingTaskManagerId> findPendingManagerToFulfill(
            ResourceProfile resourceProfile,
            Map<PendingTaskManagerId, ResourceProfile> availableResources) {
        return availableResources.entrySet().stream()
                .filter(entry -> entry.getValue().allFieldsNoLessThan(resourceProfile))
                .findAny()
                .map(Map.Entry::getKey);
    }

    private void tryFulfillRequirementsForJobWithPendingResources(
            JobID jobId,
            ResourceCounter unfulfilledRequirements,
            Map<PendingTaskManagerId, ResourceProfile> availableResources,
            ResourceAllocationResult.Builder resultBuilder) {
        for (Map.Entry<ResourceProfile, Integer> missingResource :
                unfulfilledRequirements.getResourcesWithCount()) {
            // for this strategy, all pending resources should have the same default slot resource
            final ResourceProfile effectiveProfile =
                    getEffectiveResourceProfile(
                            missingResource.getKey(), defaultSlotResourceProfile);
            for (int i = 0; i < missingResource.getValue(); i++) {
                Optional<PendingTaskManagerId> matchedPendingTaskManager =
                        findPendingManagerToFulfill(effectiveProfile, availableResources);
                if (matchedPendingTaskManager.isPresent()) {
                    availableResources.compute(
                            matchedPendingTaskManager.get(),
                            ((pendingTaskManagerId, resourceProfile) ->
                                    Preconditions.checkNotNull(resourceProfile)
                                            .subtract(effectiveProfile)));
                    resultBuilder.addAllocationOnPendingResource(
                            jobId, matchedPendingTaskManager.get(), effectiveProfile);
                } else {
                    if (totalResourceProfile.allFieldsNoLessThan(effectiveProfile)) {
                        // Add new pending task manager
                        final PendingTaskManager pendingTaskManager =
                                new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
                        resultBuilder.addPendingTaskManagerAllocate(pendingTaskManager);
                        resultBuilder.addAllocationOnPendingResource(
                                jobId,
                                pendingTaskManager.getPendingTaskManagerId(),
                                effectiveProfile);
                        availableResources.put(
                                pendingTaskManager.getPendingTaskManagerId(),
                                totalResourceProfile.subtract(effectiveProfile));
                    } else {
                        resultBuilder.addUnfulfillableJob(jobId);
                        break;
                    }
                }
            }
        }
    }
}
