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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final ResourceMatchingStrategy availableResourceMatchingStrategy;

    /**
     * Always use any matching strategy for pending resources to use as less pending workers as
     * possible, so that the rest can be canceled
     */
    private final ResourceMatchingStrategy pendingResourceMatchingStrategy =
            AnyMatchingResourceMatchingStrategy.INSTANCE;

    private final Time taskManagerTimeout;

    /** Defines the number of redundant task managers. */
    private final int redundantTaskManagerNum;

    public DefaultResourceAllocationStrategy(
            ResourceProfile totalResourceProfile,
            int numSlotsPerWorker,
            boolean evenlySpreadOutSlots,
            Time taskManagerTimeout,
            int redundantTaskManagerNum) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        totalResourceProfile, numSlotsPerWorker);
        this.availableResourceMatchingStrategy =
                evenlySpreadOutSlots
                        ? LeastUtilizationResourceMatchingStrategy.INSTANCE
                        : AnyMatchingResourceMatchingStrategy.INSTANCE;
        this.taskManagerTimeout = taskManagerTimeout;
        this.redundantTaskManagerNum = redundantTaskManagerNum;
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();

        final List<InternalResourceInfo> registeredResources =
                getAvailableResources(
                        taskManagerResourceInfoProvider, resultBuilder, blockedTaskManagerChecker);
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

        // Unlike tryFulfillRequirementsForJobWithPendingResources, which updates pendingResources
        // to the latest state after a new PendingTaskManager is created,
        // tryFulFillRedundantResources will not update pendingResources even after new
        // PendingTaskManagers are created.
        // This is because the pendingResources are no longer needed afterwards.
        tryFulFillRedundantResources(
                totalResourceProfile.multiply(redundantTaskManagerNum),
                registeredResources,
                pendingResources,
                resultBuilder);

        return resultBuilder.build();
    }

    @Override
    public ResourceReconcileResult tryReconcileClusterResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        ResourceProfile requiredRedundantResources =
                totalResourceProfile.multiply(redundantTaskManagerNum);
        ResourceReconcileResult.Builder builder = ResourceReconcileResult.builder();

        List<TaskManagerInfo> taskManagersIdleTimeout = new ArrayList<>();
        List<TaskManagerInfo> taskManagersNonTimeout = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        taskManagerResourceInfoProvider
                .getRegisteredTaskManagers()
                .forEach(
                        taskManagerInfo -> {
                            if (taskManagerInfo.isIdle()
                                    && currentTime - taskManagerInfo.getIdleSince()
                                            >= taskManagerTimeout.toMilliseconds()) {
                                taskManagersIdleTimeout.add(taskManagerInfo);
                            } else {
                                taskManagersNonTimeout.add(taskManagerInfo);
                            }
                        });

        List<PendingTaskManager> pendingTaskManagersNonUse = new ArrayList<>();
        List<PendingTaskManager> pendingTaskManagersInuse = new ArrayList<>();
        taskManagerResourceInfoProvider
                .getPendingTaskManagers()
                .forEach(
                        pendingTaskManager -> {
                            if (pendingTaskManager.getPendingSlotAllocationRecords().isEmpty()) {
                                pendingTaskManagersNonUse.add(pendingTaskManager);
                            } else {
                                pendingTaskManagersInuse.add(pendingTaskManager);
                            }
                        });

        if (taskManagersIdleTimeout.isEmpty() && pendingTaskManagersNonUse.isEmpty()) {
            // short-cut for nothing to release
            return builder.build();
        }

        ResourceProfile resourcesToKeep = ResourceProfile.ZERO;
        boolean redundantFulfilled = false;

        // check whether available resources of used (pending) task manager is enough.
        ResourceProfile availableResourcesOfNonIdle =
                getAvailableResourceOfTaskManagers(taskManagersNonTimeout);
        resourcesToKeep = resourcesToKeep.merge(availableResourcesOfNonIdle);
        if (canFulfillRequirement(requiredRedundantResources, resourcesToKeep)) {
            redundantFulfilled = true;
        } else {
            ResourceProfile availableResourcesOfNonIdlePendingTaskManager =
                    getAvailableResourceOfPendingTaskManagers(pendingTaskManagersInuse);
            resourcesToKeep = resourcesToKeep.merge(availableResourcesOfNonIdlePendingTaskManager);
        }

        // try reserve or release unused (pending) task managers
        for (TaskManagerInfo taskManagerInfo : taskManagersIdleTimeout) {
            if (redundantFulfilled
                    || canFulfillRequirement(requiredRedundantResources, resourcesToKeep)) {
                redundantFulfilled = true;
                builder.addTaskManagerToRelease(taskManagerInfo);
            } else {
                resourcesToKeep = resourcesToKeep.merge(taskManagerInfo.getAvailableResource());
            }
        }
        for (PendingTaskManager pendingTaskManager : pendingTaskManagersNonUse) {
            if (redundantFulfilled
                    || canFulfillRequirement(requiredRedundantResources, resourcesToKeep)) {
                redundantFulfilled = true;
                builder.addPendingTaskManagerToRelease(pendingTaskManager);
            } else {
                resourcesToKeep = resourcesToKeep.merge(pendingTaskManager.getUnusedResource());
            }
        }

        if (!redundantFulfilled) {
            // fulfill redundant resources
            tryFulFillRedundantResourcesWithAction(
                    requiredRedundantResources,
                    resourcesToKeep,
                    builder::addPendingTaskManagerToAllocate);
        }

        return builder.build();
    }

    private static List<InternalResourceInfo> getAvailableResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            ResourceAllocationResult.Builder resultBuilder,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        return taskManagerResourceInfoProvider.getRegisteredTaskManagers().stream()
                .filter(
                        taskManager ->
                                !blockedTaskManagerChecker.isBlockedTaskManager(
                                        taskManager.getTaskExecutorConnection().getResourceID()))
                .map(
                        taskManager ->
                                new InternalResourceInfo(
                                        taskManager.getDefaultSlotResourceProfile(),
                                        taskManager.getTotalResource(),
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
                                        pendingTaskManager.getTotalResourceProfile(),
                                        (jobId, slotProfile) ->
                                                resultBuilder.addAllocationOnPendingResource(
                                                        jobId,
                                                        pendingTaskManager
                                                                .getPendingTaskManagerId(),
                                                        slotProfile)))
                .collect(Collectors.toList());
    }

    private Collection<ResourceRequirement> tryFulfillRequirementsForJobWithResources(
            JobID jobId,
            Collection<ResourceRequirement> missingResources,
            List<InternalResourceInfo> registeredResources) {
        Collection<ResourceRequirement> outstandingRequirements = new ArrayList<>();

        for (ResourceRequirement resourceRequirement : missingResources) {
            int numMissingRequirements =
                    availableResourceMatchingStrategy.tryFulfilledRequirementWithResource(
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
                    pendingResourceMatchingStrategy.tryFulfilledRequirementWithResource(
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
                                    totalResourceProfile,
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

    private void tryFulFillRedundantResources(
            ResourceProfile requiredRedundantResource,
            List<InternalResourceInfo> availableRegisteredResources,
            List<InternalResourceInfo> availablePendingResources,
            ResourceAllocationResult.Builder resultBuilder) {
        ResourceProfile totalAvailableResources =
                Stream.concat(
                                availableRegisteredResources.stream(),
                                availablePendingResources.stream())
                        .map(internalResourceInfo -> internalResourceInfo.availableProfile)
                        .reduce(ResourceProfile.ZERO, ResourceProfile::merge);

        tryFulFillRedundantResourcesWithAction(
                requiredRedundantResource,
                totalAvailableResources,
                resultBuilder::addPendingTaskManagerAllocate);
    }

    private void tryFulFillRedundantResourcesWithAction(
            ResourceProfile requiredRedundantResource,
            ResourceProfile totalAvailableResources,
            Consumer<? super PendingTaskManager> fulfillAction) {
        while (!canFulfillRequirement(requiredRedundantResource, totalAvailableResources)) {
            PendingTaskManager pendingTaskManager =
                    new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
            fulfillAction.accept(pendingTaskManager);
            totalAvailableResources = totalAvailableResources.merge(totalResourceProfile);
        }
    }

    private ResourceProfile getAvailableResourceOfTaskManagers(List<TaskManagerInfo> taskManagers) {
        return taskManagers.stream()
                .map(TaskManagerInfo::getAvailableResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    private ResourceProfile getAvailableResourceOfPendingTaskManagers(
            List<PendingTaskManager> pendingTaskManagers) {
        return pendingTaskManagers.stream()
                .map(PendingTaskManager::getUnusedResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    private static class InternalResourceInfo {
        private final ResourceProfile defaultSlotProfile;
        private final BiConsumer<JobID, ResourceProfile> allocationConsumer;
        private final ResourceProfile totalProfile;
        private ResourceProfile availableProfile;
        private double utilization;

        InternalResourceInfo(
                ResourceProfile defaultSlotProfile,
                ResourceProfile totalProfile,
                ResourceProfile availableProfile,
                BiConsumer<JobID, ResourceProfile> allocationConsumer) {
            Preconditions.checkState(!defaultSlotProfile.equals(ResourceProfile.UNKNOWN));
            Preconditions.checkState(!totalProfile.equals(ResourceProfile.UNKNOWN));
            Preconditions.checkState(!availableProfile.equals(ResourceProfile.UNKNOWN));
            this.defaultSlotProfile = defaultSlotProfile;
            this.totalProfile = totalProfile;
            this.availableProfile = availableProfile;
            this.allocationConsumer = allocationConsumer;
            this.utilization = updateUtilization();
        }

        boolean tryAllocateSlotForJob(JobID jobId, ResourceProfile requirement) {
            final ResourceProfile effectiveProfile =
                    getEffectiveResourceProfile(requirement, defaultSlotProfile);
            if (availableProfile.allFieldsNoLessThan(effectiveProfile)) {
                availableProfile = availableProfile.subtract(effectiveProfile);
                allocationConsumer.accept(jobId, effectiveProfile);
                utilization = updateUtilization();
                return true;
            } else {
                return false;
            }
        }

        private double updateUtilization() {
            double cpuUtilization =
                    totalProfile
                                    .getCpuCores()
                                    .subtract(availableProfile.getCpuCores())
                                    .getValue()
                                    .doubleValue()
                            / totalProfile.getCpuCores().getValue().doubleValue();
            double memoryUtilization =
                    (double)
                                    totalProfile
                                            .getTotalMemory()
                                            .subtract(availableProfile.getTotalMemory())
                                            .getBytes()
                            / totalProfile.getTotalMemory().getBytes();
            return Math.max(cpuUtilization, memoryUtilization);
        }
    }

    private interface ResourceMatchingStrategy {

        int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources,
                int numUnfulfilled,
                ResourceProfile requiredResource,
                JobID jobId);
    }

    private enum AnyMatchingResourceMatchingStrategy implements ResourceMatchingStrategy {
        INSTANCE;

        @Override
        public int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources,
                int numUnfulfilled,
                ResourceProfile requiredResource,
                JobID jobId) {
            final Iterator<InternalResourceInfo> internalResourceInfoItr =
                    internalResources.iterator();
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
    }

    private enum LeastUtilizationResourceMatchingStrategy implements ResourceMatchingStrategy {
        INSTANCE;

        @Override
        public int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources,
                int numUnfulfilled,
                ResourceProfile requiredResource,
                JobID jobId) {
            if (internalResources.isEmpty()) {
                return numUnfulfilled;
            }

            Queue<InternalResourceInfo> resourceInfoInUtilizationOrder =
                    new PriorityQueue<>(
                            internalResources.size(),
                            Comparator.comparingDouble(i -> i.utilization));
            resourceInfoInUtilizationOrder.addAll(internalResources);

            while (numUnfulfilled > 0 && !resourceInfoInUtilizationOrder.isEmpty()) {
                final InternalResourceInfo currentTaskManager =
                        resourceInfoInUtilizationOrder.poll();

                if (currentTaskManager.tryAllocateSlotForJob(jobId, requiredResource)) {
                    numUnfulfilled--;

                    // ignore non resource task managers to reduce the overhead of insert.
                    if (!currentTaskManager.availableProfile.equals(ResourceProfile.ZERO)) {
                        resourceInfoInUtilizationOrder.add(currentTaskManager);
                    }
                }
            }
            return numUnfulfilled;
        }
    }
}
