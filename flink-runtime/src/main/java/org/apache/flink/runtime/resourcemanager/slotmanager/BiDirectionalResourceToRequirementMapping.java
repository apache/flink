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
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** A bi-directional mapping between required and acquired resources. */
class BiDirectionalResourceToRequirementMapping {
    private final Map<LoadableResourceProfile, ResourceCounter> requirementToFulfillingResources =
            new HashMap<>();
    private final Map<LoadableResourceProfile, ResourceCounter> resourceToFulfilledRequirement =
            new HashMap<>();

    public void incrementCount(
            LoadableResourceProfile requirement, LoadableResourceProfile resource, int increment) {
        Preconditions.checkNotNull(requirement);
        Preconditions.checkNotNull(resource);
        Preconditions.checkArgument(increment > 0);
        internalIncrementCount(requirementToFulfillingResources, requirement, resource, increment);
        internalIncrementCount(resourceToFulfilledRequirement, resource, requirement, increment);
    }

    public void decrementCount(
            LoadableResourceProfile requirement, LoadableResourceProfile resource, int decrement) {
        Preconditions.checkNotNull(requirement);
        Preconditions.checkNotNull(resource);
        Preconditions.checkArgument(decrement > 0);
        internalDecrementCount(requirementToFulfillingResources, requirement, resource, decrement);
        internalDecrementCount(resourceToFulfilledRequirement, resource, requirement, decrement);
    }

    private static void internalIncrementCount(
            Map<LoadableResourceProfile, ResourceCounter> primaryMap,
            LoadableResourceProfile primaryKey,
            LoadableResourceProfile secondaryKey,
            int increment) {
        primaryMap.compute(
                primaryKey,
                (loadableResourceProfile, resourceCounter) -> {
                    if (resourceCounter == null) {
                        return ResourceCounter.withResource(secondaryKey, increment);
                    } else {
                        return resourceCounter.add(secondaryKey, increment);
                    }
                });
    }

    private static void internalDecrementCount(
            Map<LoadableResourceProfile, ResourceCounter> primaryMap,
            LoadableResourceProfile primaryKey,
            LoadableResourceProfile secondaryKey,
            int decrement) {
        primaryMap.compute(
                primaryKey,
                (loadableResourceProfile, resourceCounter) -> {
                    Preconditions.checkState(
                            resourceCounter != null,
                            "Attempting to decrement count of %s->%s, but primary key was unknown.",
                            loadableResourceProfile,
                            secondaryKey);
                    final ResourceCounter newCounter =
                            resourceCounter.subtract(secondaryKey, decrement);
                    return newCounter.isEmpty() ? null : newCounter;
                });
    }

    public ResourceCounter getLoadableResourcesFulfilling(LoadableResourceProfile requirement) {
        Preconditions.checkNotNull(requirement);
        return requirementToFulfillingResources.getOrDefault(requirement, ResourceCounter.empty());
    }

    public ResourceCounter getLoadableRequirementsFulfilledBy(LoadableResourceProfile resource) {
        Preconditions.checkNotNull(resource);
        return resourceToFulfilledRequirement.getOrDefault(resource, ResourceCounter.empty());
    }

    public Set<LoadableResourceProfile> getAllLoadableResourceProfiles() {
        return resourceToFulfilledRequirement.keySet();
    }

    public Set<LoadableResourceProfile> getAllRequirementLoadableProfiles() {
        return requirementToFulfillingResources.keySet();
    }

    public int getNumFulfillingLoadableResources(LoadableResourceProfile requirement) {
        Preconditions.checkNotNull(requirement);
        return requirementToFulfillingResources
                .getOrDefault(requirement, ResourceCounter.empty())
                .getTotalResourceCount();
    }

    public int getNumFulfilledLoadableRequirements(LoadableResourceProfile resource) {
        Preconditions.checkNotNull(resource);
        return resourceToFulfilledRequirement
                .getOrDefault(resource, ResourceCounter.empty())
                .getTotalResourceCount();
    }

    @VisibleForTesting
    boolean isEmpty() {
        return requirementToFulfillingResources.isEmpty()
                && resourceToFulfilledRequirement.isEmpty();
    }

    @Override
    public String toString() {
        return "BiDirectionalResourceToRequirementMapping{"
                + "requirementToFulfillingResources="
                + requirementToFulfillingResources
                + ", resourceToFulfilledRequirement="
                + resourceToFulfilledRequirement
                + '}';
    }
}
