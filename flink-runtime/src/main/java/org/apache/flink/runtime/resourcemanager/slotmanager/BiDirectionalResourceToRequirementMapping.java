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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A bi-directional mapping between required and acquired resources.
 */
class BiDirectionalResourceToRequirementMapping {
	private final Map<ResourceProfile, ResourceCounter> requirementToFulfillingResources = new HashMap<>();
	private final Map<ResourceProfile, ResourceCounter> resourceToFulfilledRequirement = new HashMap<>();

	public void incrementCount(ResourceProfile requirement, ResourceProfile resource, int increment) {
		Preconditions.checkNotNull(requirement);
		Preconditions.checkNotNull(resource);
		Preconditions.checkArgument(increment > 0);
		internalIncrementCount(requirementToFulfillingResources, requirement, resource, increment);
		internalIncrementCount(resourceToFulfilledRequirement, resource, requirement, increment);
	}

	public void decrementCount(ResourceProfile requirement, ResourceProfile resource, int decrement) {
		Preconditions.checkNotNull(requirement);
		Preconditions.checkNotNull(resource);
		Preconditions.checkArgument(decrement > 0);
		internalDecrementCount(requirementToFulfillingResources, requirement, resource, decrement);
		internalDecrementCount(resourceToFulfilledRequirement, resource, requirement, decrement);
	}

	private static void internalIncrementCount(Map<ResourceProfile, ResourceCounter> primaryMap, ResourceProfile primaryKey, ResourceProfile secondaryKey, int increment) {
		primaryMap
			.computeIfAbsent(primaryKey, ignored -> new ResourceCounter())
			.incrementCount(secondaryKey, increment);
	}

	private static void internalDecrementCount(Map<ResourceProfile, ResourceCounter> primaryMap, ResourceProfile primaryKey, ResourceProfile secondaryKey, int decrement) {
		primaryMap.compute(
			primaryKey,
			(resourceProfile, resourceCounter) -> {
				Preconditions.checkState(resourceCounter != null, "Attempting to decrement count of %s->%s, but primary key was unknown.", resourceProfile, secondaryKey);
				resourceCounter.decrementCount(secondaryKey, decrement);
				return resourceCounter.isEmpty() ? null : resourceCounter;
			});
	}

	public Map<ResourceProfile, Integer> getResourcesFulfilling(ResourceProfile requirement) {
		Preconditions.checkNotNull(requirement);
		return requirementToFulfillingResources.getOrDefault(requirement, ResourceCounter.EMPTY).getResourceProfilesWithCount();
	}

	public Map<ResourceProfile, Integer> getRequirementsFulfilledBy(ResourceProfile resource) {
		Preconditions.checkNotNull(resource);
		return resourceToFulfilledRequirement.getOrDefault(resource, ResourceCounter.EMPTY).getResourceProfilesWithCount();
	}

	public Set<ResourceProfile> getAllResourceProfiles() {
		return resourceToFulfilledRequirement.keySet();
	}

	public Set<ResourceProfile> getAllRequirementProfiles() {
		return requirementToFulfillingResources.keySet();
	}

	public int getNumFulfillingResources(ResourceProfile requirement) {
		Preconditions.checkNotNull(requirement);
		return requirementToFulfillingResources
			.getOrDefault(requirement, ResourceCounter.EMPTY)
			.getResourceCount();
	}

	public int getNumFulfilledRequirements(ResourceProfile resource) {
		Preconditions.checkNotNull(resource);
		return resourceToFulfilledRequirement
			.getOrDefault(resource, ResourceCounter.EMPTY)
			.getResourceCount();
	}

	@VisibleForTesting
	boolean isEmpty() {
		return requirementToFulfillingResources.isEmpty() && resourceToFulfilledRequirement.isEmpty();
	}
}
