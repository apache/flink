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
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Tracks resource for a single job. */
class JobScopedResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(JobScopedResourceTracker.class);

    // only for logging purposes
    private final JobID jobId;

    private final BiDirectionalResourceToRequirementMapping resourceToRequirementMapping =
            new BiDirectionalResourceToRequirementMapping();

    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    private ResourceCounter resourceRequirements = ResourceCounter.empty();
    private ResourceCounter excessResources = ResourceCounter.empty();

    JobScopedResourceTracker(JobID jobId) {
        this.jobId = Preconditions.checkNotNull(jobId);
    }

    public void notifyResourceRequirements(
            Collection<ResourceRequirement> newResourceRequirements) {
        Preconditions.checkNotNull(newResourceRequirements);

        resourceRequirements = ResourceCounter.empty();
        for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
            resourceRequirements =
                    resourceRequirements.add(
                            newResourceRequirement.getLoadableResourceProfile(),
                            newResourceRequirement.getNumberOfRequiredSlots());
        }
        findExcessSlots();
        tryAssigningExcessSlots();
    }

    public void notifyAcquiredResource(LoadableResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        final Optional<LoadableResourceProfile> matchingRequirement =
                findMatchingRequirement(resourceProfile);
        if (matchingRequirement.isPresent()) {
            resourceToRequirementMapping.incrementCount(
                    matchingRequirement.get(), resourceProfile, 1);
        } else {
            LOG.debug("Job {} acquired excess resource {}.", resourceProfile, jobId);
            excessResources = excessResources.add(resourceProfile);
        }
    }

    private Optional<LoadableResourceProfile> findMatchingRequirement(
            LoadableResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                resourceRequirements,
                resourceToRequirementMapping::getNumFulfillingLoadableResources);
    }

    public void notifyLostResource(LoadableResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        if (excessResources.getLoadableResourceCount(resourceProfile) > 0) {
            LOG.trace("Job {} lost excess resource {}.", jobId, resourceProfile);
            excessResources = excessResources.subtract(resourceProfile);
            return;
        }

        Set<LoadableResourceProfile> fulfilledRequirements =
                resourceToRequirementMapping
                        .getLoadableRequirementsFulfilledBy(resourceProfile)
                        .getLoadableResources();
        LOG.debug(
                "notifyLostResource resourceToRequirementMapping {}", resourceToRequirementMapping);
        if (!fulfilledRequirements.isEmpty()) {
            // deduct the resource from any requirement
            // from a correctness standpoint the choice is arbitrary
            // from a resource utilization standpoint it could make sense to search for the
            // requirement with the largest
            // distance to the resource profile (i.e., the smallest requirement), but it may not
            // matter since we are
            // likely to get back a similarly-sized resource later on
            LoadableResourceProfile assignedRequirement = fulfilledRequirements.iterator().next();

            resourceToRequirementMapping.decrementCount(assignedRequirement, resourceProfile, 1);

            tryAssigningExcessSlots();
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Job %s lost a resource %s but no such resource was tracked.",
                            jobId, resourceProfile));
        }
    }

    public Collection<ResourceRequirement> getMissingResources() {
        final Collection<ResourceRequirement> missingResources = new ArrayList<>();
        LOG.debug("getMissingResources, resourceRequirements: {}", resourceRequirements);
        for (Map.Entry<LoadableResourceProfile, Integer> requirement :
                resourceRequirements.getLoadableResourcesWithCount()) {
            LoadableResourceProfile requirementProfile = requirement.getKey();

            int numRequiredResources = requirement.getValue();
            int numAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingLoadableResources(
                            requirementProfile);

            LOG.debug(
                    "getMissingResources resourceToRequirementMapping {}",
                    resourceToRequirementMapping);
            if (numAcquiredResources < numRequiredResources) {
                missingResources.add(
                        ResourceRequirement.create(
                                requirementProfile, numRequiredResources - numAcquiredResources));
            }
        }
        return missingResources;
    }

    public Collection<ResourceRequirement> getAcquiredResources() {
        final Set<LoadableResourceProfile> knownResourceProfiles = new HashSet<>();
        knownResourceProfiles.addAll(resourceToRequirementMapping.getAllLoadableResourceProfiles());
        knownResourceProfiles.addAll(excessResources.getLoadableResources());

        final List<ResourceRequirement> acquiredResources = new ArrayList<>();
        for (LoadableResourceProfile knownResourceProfile : knownResourceProfiles) {
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfilledLoadableRequirements(
                                    knownResourceProfile)
                            + excessResources.getLoadableResourceCount(knownResourceProfile);
            ResourceRequirement resourceRequirement =
                    ResourceRequirement.create(knownResourceProfile, numTotalAcquiredResources);
            acquiredResources.add(resourceRequirement);
        }

        return acquiredResources;
    }

    public boolean isEmpty() {
        return resourceRequirements.isEmpty() && excessResources.isEmpty();
    }

    public boolean isRequirementEmpty() {
        return resourceRequirements.isEmpty();
    }

    private void findExcessSlots() {
        final Collection<ExcessResource> excessResources = new ArrayList<>();

        for (LoadableResourceProfile requirementProfile :
                resourceToRequirementMapping.getAllRequirementLoadableProfiles()) {
            int numTotalRequiredResources =
                    resourceRequirements.getLoadableResourceCount(requirementProfile);
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingLoadableResources(
                            requirementProfile);

            if (numTotalAcquiredResources > numTotalRequiredResources) {
                int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;

                for (Map.Entry<LoadableResourceProfile, Integer> acquiredResource :
                        resourceToRequirementMapping
                                .getLoadableResourcesFulfilling(requirementProfile)
                                .getLoadableResourcesWithCount()) {
                    LoadableResourceProfile acquiredResourceProfile = acquiredResource.getKey();
                    int numAcquiredResources = acquiredResource.getValue();

                    if (numAcquiredResources <= numExcessResources) {
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numAcquiredResources));

                        numExcessResources -= numAcquiredResources;
                    } else {
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numExcessResources));
                        break;
                    }
                }
            }
        }

        if (!excessResources.isEmpty()) {
            LOG.debug("Detected excess resources for job {}: {}", jobId, excessResources);
            for (ExcessResource excessResource : excessResources) {
                resourceToRequirementMapping.decrementCount(
                        excessResource.requirementProfile,
                        excessResource.resourceProfile,
                        excessResource.numExcessResources);
                this.excessResources =
                        this.excessResources.add(
                                excessResource.resourceProfile, excessResource.numExcessResources);
            }
        }
    }

    private void tryAssigningExcessSlots() {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} before re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }

        ResourceCounter assignedResources = ResourceCounter.empty();
        for (Map.Entry<LoadableResourceProfile, Integer> excessResource :
                excessResources.getLoadableResourcesWithCount()) {
            for (int i = 0; i < excessResource.getValue(); i++) {
                final LoadableResourceProfile resourceProfile = excessResource.getKey();
                final Optional<LoadableResourceProfile> matchingRequirement =
                        findMatchingRequirement(resourceProfile);
                if (matchingRequirement.isPresent()) {
                    resourceToRequirementMapping.incrementCount(
                            matchingRequirement.get(), resourceProfile, 1);
                    assignedResources = assignedResources.add(resourceProfile, 1);
                } else {
                    break;
                }
            }
        }

        for (Map.Entry<LoadableResourceProfile, Integer> assignedResource :
                assignedResources.getLoadableResourcesWithCount()) {
            excessResources =
                    excessResources.subtract(
                            assignedResource.getKey(), assignedResource.getValue());
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} after re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }
    }

    @Override
    public String toString() {
        return "JobScopedResourceTracker{"
                + "jobId="
                + jobId
                + ", resourceToRequirementMapping="
                + resourceToRequirementMapping
                + ", requirementMatcher="
                + requirementMatcher
                + ", resourceRequirements="
                + resourceRequirements
                + ", excessResources="
                + excessResources
                + '}';
    }

    private static class ExcessResource {
        private final LoadableResourceProfile requirementProfile;
        private final LoadableResourceProfile resourceProfile;
        private final int numExcessResources;

        private ExcessResource(
                LoadableResourceProfile requirementProfile,
                LoadableResourceProfile resourceProfile,
                int numExcessResources) {
            this.requirementProfile = requirementProfile;
            this.resourceProfile = resourceProfile;
            this.numExcessResources = numExcessResources;
        }

        @Override
        public String toString() {
            return "ExcessResource{"
                    + "numExcessResources="
                    + numExcessResources
                    + ", requirementProfile="
                    + requirementProfile
                    + ", resourceProfile="
                    + resourceProfile
                    + '}';
        }
    }
}
