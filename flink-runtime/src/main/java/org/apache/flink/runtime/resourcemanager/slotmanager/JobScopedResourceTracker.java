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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.runtime.slots.ResourceRequirement;
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

/**
 * Tracks resource for a single job.
 */
class JobScopedResourceTracker {

	private static final Logger LOG = LoggerFactory.getLogger(JobScopedResourceTracker.class);

	// only for logging purposes
	private final JobID jobId;

	private final ResourceCounter resourceRequirements = new ResourceCounter();
	private final BiDirectionalResourceToRequirementMapping resourceToRequirementMapping = new BiDirectionalResourceToRequirementMapping();
	private final ResourceCounter excessResources = new ResourceCounter();

	JobScopedResourceTracker(JobID jobId) {
		this.jobId = Preconditions.checkNotNull(jobId);
	}

	public void notifyResourceRequirements(Collection<ResourceRequirement> newResourceRequirements) {
		Preconditions.checkNotNull(newResourceRequirements);

		resourceRequirements.clear();
		for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
			resourceRequirements.incrementCount(newResourceRequirement.getResourceProfile(), newResourceRequirement.getNumberOfRequiredSlots());
		}
		findExcessSlots();
		tryAssigningExcessSlots();
	}

	public void notifyAcquiredResource(ResourceProfile resourceProfile) {
		Preconditions.checkNotNull(resourceProfile);
		final Optional<ResourceProfile> matchingRequirement = findMatchingRequirement(resourceProfile);
		if (matchingRequirement.isPresent()) {
			resourceToRequirementMapping.incrementCount(matchingRequirement.get(), resourceProfile, 1);
		} else {
			LOG.debug("Job {} acquired excess resource {}.", resourceProfile, jobId);
			excessResources.incrementCount(resourceProfile, 1);
		}
	}

	private Optional<ResourceProfile> findMatchingRequirement(ResourceProfile resourceProfile) {
		for (Map.Entry<ResourceProfile, Integer> requirementCandidate : resourceRequirements.getResourceProfilesWithCount().entrySet()) {
			ResourceProfile requirementProfile = requirementCandidate.getKey();

			// beware the order when matching resources to requirements, because ResourceProfile.UNKNOWN (which only
			// occurs as a requirement) does not match any resource!
			if (resourceProfile.isMatching(requirementProfile) && requirementCandidate.getValue() > resourceToRequirementMapping.getNumFulfillingResources(requirementProfile)) {
				return Optional.of(requirementProfile);
			}
		}
		return Optional.empty();
	}

	public void notifyLostResource(ResourceProfile resourceProfile) {
		Preconditions.checkNotNull(resourceProfile);
		if (excessResources.getResourceCount(resourceProfile) > 0) {
			LOG.trace("Job {} lost excess resource {}.", jobId, resourceProfile);
			excessResources.decrementCount(resourceProfile, 1);
			return;
		}

		Set<ResourceProfile> fulfilledRequirements = resourceToRequirementMapping.getRequirementsFulfilledBy(resourceProfile).keySet();

		if (!fulfilledRequirements.isEmpty()) {
			// deduct the resource from any requirement
			// from a correctness standpoint the choice is arbitrary
			// from a resource utilization standpoint it could make sense to search for the requirement with the largest
			// distance to the resource profile (i.e., the smallest requirement), but it may not matter since we are
			// likely to get back a similarly-sized resource later on
			ResourceProfile assignedRequirement = fulfilledRequirements.iterator().next();

			resourceToRequirementMapping.decrementCount(assignedRequirement, resourceProfile, 1);

			tryAssigningExcessSlots();
		} else {
			throw new IllegalStateException(String.format("Job %s lost a resource %s but no such resource was tracked.", jobId, resourceProfile));
		}
	}

	public Collection<ResourceRequirement> getMissingResources() {
		final Collection<ResourceRequirement> missingResources = new ArrayList<>();
		for (Map.Entry<ResourceProfile, Integer> requirement : resourceRequirements.getResourceProfilesWithCount().entrySet()) {
			ResourceProfile requirementProfile = requirement.getKey();

			int numRequiredResources = requirement.getValue();
			int numAcquiredResources = resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

			if (numAcquiredResources < numRequiredResources) {
				missingResources.add(ResourceRequirement.create(requirementProfile, numRequiredResources - numAcquiredResources));
			}

		}
		return missingResources;
	}

	public Collection<ResourceRequirement> getAcquiredResources() {
		final Set<ResourceProfile> knownResourceProfiles = new HashSet<>();
		knownResourceProfiles.addAll(resourceToRequirementMapping.getAllResourceProfiles());
		knownResourceProfiles.addAll(excessResources.getResourceProfiles());

		final List<ResourceRequirement> acquiredResources = new ArrayList<>();
		for (ResourceProfile knownResourceProfile : knownResourceProfiles) {
			int numTotalAcquiredResources = resourceToRequirementMapping.getNumFulfilledRequirements(knownResourceProfile) + excessResources.getResourceCount(knownResourceProfile);
			ResourceRequirement resourceRequirement = ResourceRequirement.create(knownResourceProfile, numTotalAcquiredResources);
			acquiredResources.add(resourceRequirement);
		}

		return acquiredResources;
	}

	public boolean isEmpty() {
		return resourceRequirements.isEmpty() && excessResources.isEmpty();
	}

	private void findExcessSlots() {
		final Collection<ExcessResource> excessResources = new ArrayList<>();

		for (ResourceProfile requirementProfile : resourceToRequirementMapping.getAllRequirementProfiles()) {
			int numTotalRequiredResources = resourceRequirements.getResourceCount(requirementProfile);
			int numTotalAcquiredResources = resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

			if (numTotalAcquiredResources > numTotalRequiredResources) {
				int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;

				for (Map.Entry<ResourceProfile, Integer> acquiredResource : resourceToRequirementMapping.getResourcesFulfilling(requirementProfile).entrySet()) {
					ResourceProfile acquiredResourceProfile = acquiredResource.getKey();
					int numAcquiredResources = acquiredResource.getValue();

					if (numAcquiredResources <= numExcessResources) {
						excessResources.add(new ExcessResource(requirementProfile, acquiredResourceProfile, numAcquiredResources));

						numExcessResources -= numAcquiredResources;
					} else {
						excessResources.add(new ExcessResource(requirementProfile, acquiredResourceProfile, numExcessResources));
						break;
					}
				}
			}
		}

		if (!excessResources.isEmpty()) {
			LOG.debug("Detected excess resources for job {}: {}", jobId, excessResources);
			for (ExcessResource excessResource : excessResources) {
				resourceToRequirementMapping.decrementCount(excessResource.requirementProfile, excessResource.resourceProfile, excessResource.numExcessResources);
				this.excessResources.incrementCount(excessResource.resourceProfile, excessResource.numExcessResources);
			}
		}
	}

	private void tryAssigningExcessSlots() {
		if (LOG.isTraceEnabled()) {
			LOG.trace("There are {} excess resources for job {} before re-assignment.", jobId, excessResources.getResourceCount());
		}
		// this is a quick-and-dirty solution; in the worse case we copy the excessResources map twice
		ResourceCounter copy = excessResources.copy();
		excessResources.clear();
		for (Map.Entry<ResourceProfile, Integer> resourceProfileIntegerEntry : copy.getResourceProfilesWithCount().entrySet()) {
			for (int x = 0; x < resourceProfileIntegerEntry.getValue(); x++) {
				// try making use of this resource again; any excess resources will be added again to excessResources
				notifyAcquiredResource(resourceProfileIntegerEntry.getKey());
			}
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("There are {} excess resources for job {} after re-assignment.", jobId, excessResources.getResourceCount());
		}
	}

	private static class ExcessResource {
		private final ResourceProfile requirementProfile;
		private final ResourceProfile resourceProfile;
		private final int numExcessResources;

		private ExcessResource(ResourceProfile requirementProfile, ResourceProfile resourceProfile, int numExcessResources) {
			this.requirementProfile = requirementProfile;
			this.resourceProfile = resourceProfile;
			this.numExcessResources = numExcessResources;
		}
	}

}
