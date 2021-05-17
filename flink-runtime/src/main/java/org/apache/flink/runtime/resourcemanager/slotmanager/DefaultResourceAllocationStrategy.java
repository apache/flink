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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
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
 * efficiency. In the worst case, for each distinctly profiled requirement it checks all registered
 * and pending resources. Further optimization requires complex data structures for ordering
 * multi-dimensional resource profiles. The complexity is not necessary.
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

        final List<InternalResourceInfo> registeredResources =
                getRegisteredResources(taskManagerResourceInfoProvider, resultBuilder);
        final List<InternalResourceInfo> pendingResources =
                getPendingResources(taskManagerResourceInfoProvider, resultBuilder);

        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            final Collection<ResourceRequirement> unfulfilledJobRequirements =
                    tryFulfillRequirementsForJobWithResources(
                            jobId, resourceRequirements.getValue(), registeredResources);

            if (!unfulfilledJobRequirements.isEmpty()) {
                tryFulfillRequirementsForJobWithPendingResources(
                        jobId, unfulfilledJobRequirements, pendingResources, resultBuilder);
            }
        }
        return resultBuilder.build();
    }

    private static List<InternalResourceInfo> getRegisteredResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            ResourceAllocationResult.Builder resultBuilder) {
        return taskManagerResourceInfoProvider.getRegisteredTaskManagers().stream()
                .map(
                        taskManager ->
                                new InternalResourceInfo(
                                        taskManager.getDefaultSlotResourceProfile(),
                                        taskManager.getAvailableResource(),
                                        (jobId, slotProfile) ->
                                                resultBuilder.addAllocationOnRegisteredResource(
                                                        jobId,
                                                        taskManager.getInstanceId(),
                                                        slotProfile)))
                .collect(Collectors.toList());
    }

    private static List<InternalResourceInfo> getPendingResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            ResourceAllocationResult.Builder resultBuilder) {
        return taskManagerResourceInfoProvider.getPendingTaskManagers().stream()
                .map(
                        pendingTaskManager ->
                                new InternalResourceInfo(
                                        pendingTaskManager.getDefaultSlotResourceProfile(),
                                        pendingTaskManager.getTotalResourceProfile(),
                                        (jobId, slotProfile) ->
                                                resultBuilder.addAllocationOnPendingResource(
                                                        jobId,
                                                        pendingTaskManager
                                                                .getPendingTaskManagerId(),
                                                        slotProfile)))
                .collect(Collectors.toList());
    }

    private static int tryFulfilledRequirementWithResource(
            List<InternalResourceInfo> internalResource,
            int numUnfulfilled,
            ResourceProfile requiredResource,
            JobID jobId) {
        final Iterator<InternalResourceInfo> internalResourceInfoItr = internalResource.iterator();
        while (numUnfulfilled > 0 && internalResourceInfoItr.hasNext()) {
            final InternalResourceInfo currentTaskManager = internalResourceInfoItr.next();
            while (numUnfulfilled > 0
                    && currentTaskManager.tryAllocateSlotForJob(jobId, requiredResource)) {
                numUnfulfilled--;
            }
            if (currentTaskManager.availableProfile.equals(ResourceProfile.ZERO)) {
                internalResourceInfoItr.remove();
            }
        }
        return numUnfulfilled;
    }

    private static Collection<ResourceRequirement> tryFulfillRequirementsForJobWithResources(
            JobID jobId,
            Collection<ResourceRequirement> missingResources,
            List<InternalResourceInfo> registeredResources) {
        Collection<ResourceRequirement> outstandingRequirements = new ArrayList<>();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingRequirements =
                    tryFulfilledRequirementWithResource(
                            registeredResources,
                            resourceRequirement.getNumberOfRequiredSlots(),
                            resourceRequirement.getResourceProfile(),
                            jobId);
            if (numMissingRequirements > 0) {
                outstandingRequirements.add(
                        ResourceRequirement.create(
                                resourceRequirement.getResourceProfile(), numMissingRequirements));
            }
        }
        return outstandingRequirements;
    }

    private static boolean canFulfillRequirement(
            ResourceProfile requirement, ResourceProfile resourceProfile) {
        return resourceProfile.allFieldsNoLessThan(requirement);
    }

    private void tryFulfillRequirementsForJobWithPendingResources(
            JobID jobId,
            Collection<ResourceRequirement> unfulfilledRequirements,
            List<InternalResourceInfo> availableResources,
            ResourceAllocationResult.Builder resultBuilder) {
        for (ResourceRequirement missingResource : unfulfilledRequirements) {
            // for this strategy, all pending resources should have the same default slot resource
            final ResourceProfile effectiveProfile =
                    getEffectiveResourceProfile(
                            missingResource.getResourceProfile(), defaultSlotResourceProfile);
            int numUnfulfilled =
                    tryFulfilledRequirementWithResource(
                            availableResources,
                            missingResource.getNumberOfRequiredSlots(),
                            missingResource.getResourceProfile(),
                            jobId);

            if (!totalResourceProfile.allFieldsNoLessThan(effectiveProfile)) {
                // Can not fulfill this resource type will the default worker.
                resultBuilder.addUnfulfillableJob(jobId);
                continue;
            }

            while (numUnfulfilled > 0) {
                // Circularly add new pending task manager
                final PendingTaskManager newPendingTaskManager =
                        new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
                resultBuilder.addPendingTaskManagerAllocate(newPendingTaskManager);
                ResourceProfile remainResource = totalResourceProfile;
                while (numUnfulfilled > 0
                        && canFulfillRequirement(effectiveProfile, remainResource)) {
                    numUnfulfilled--;
                    resultBuilder.addAllocationOnPendingResource(
                            jobId,
                            newPendingTaskManager.getPendingTaskManagerId(),
                            effectiveProfile);
                    remainResource = remainResource.subtract(effectiveProfile);
                }
                if (!remainResource.equals(ResourceProfile.ZERO)) {
                    availableResources.add(
                            new InternalResourceInfo(
                                    defaultSlotResourceProfile,
                                    remainResource,
                                    (jobID, slotProfile) ->
                                            resultBuilder.addAllocationOnPendingResource(
                                                    jobID,
                                                    newPendingTaskManager.getPendingTaskManagerId(),
                                                    slotProfile)));
                }
            }
        }
    }

    private static class InternalResourceInfo {
        private final ResourceProfile defaultSlotProfile;
        private final BiConsumer<JobID, ResourceProfile> allocationConsumer;
        private ResourceProfile availableProfile;

        InternalResourceInfo(
                ResourceProfile defaultSlotProfile,
                ResourceProfile availableProfile,
                BiConsumer<JobID, ResourceProfile> allocationConsumer) {
            this.defaultSlotProfile = defaultSlotProfile;
            this.availableProfile = availableProfile;
            this.allocationConsumer = allocationConsumer;
        }

        boolean tryAllocateSlotForJob(JobID jobId, ResourceProfile requirement) {
            final ResourceProfile effectiveProfile =
                    getEffectiveResourceProfile(requirement, defaultSlotProfile);
            if (availableProfile.allFieldsNoLessThan(effectiveProfile)) {
                availableProfile = availableProfile.subtract(effectiveProfile);
                allocationConsumer.accept(jobId, effectiveProfile);
                return true;
            } else {
                return false;
            }
        }
    }
}
