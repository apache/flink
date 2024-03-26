/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Counter for {@link LoadableResourceProfile LoadableResourceProfiles}. This class is immutable.
 *
 * <p>ResourceCounter contains a set of {@link LoadableResourceProfile LoadableResourceProfiles} and
 * their associated counts. The counts are always positive (> 0).
 */
public final class ResourceCounter implements WeightLoadable {

    public static final Logger LOG = LoggerFactory.getLogger(ResourceCounter.class);

    private final Map<LoadableResourceProfile, Integer> resources;

    private ResourceCounter(Map<LoadableResourceProfile, Integer> resources) {
        this.resources = Collections.unmodifiableMap(resources);
    }

    /**
     * Number of resources with the given {@link LoadableResourceProfile}.
     *
     * @param resourceProfile resourceProfile with empty loading (default) for which to look up the
     *     count
     * @return number of resources with the given resourceProfile or {@code 0} if the loadable
     *     resource profile does not exist
     */
    @VisibleForTesting
    public int getResourceCount(ResourceProfile resourceProfile) {
        return resources.getOrDefault(resourceProfile.toEmptyLoadsResourceProfile(), 0);
    }

    /**
     * Number of resources with the given {@link LoadableResourceProfile}.
     *
     * @param loadableResourceProfile loadable resource profile for which to look up the count
     * @return number of resources with the given loadableResourceProfile or {@code 0} if the
     *     loadable resource profile does not exist
     */
    public int getLoadableResourceCount(LoadableResourceProfile loadableResourceProfile) {
        return resources.getOrDefault(loadableResourceProfile, 0);
    }

    /**
     * Computes the total number of loadable resources in this counter.
     *
     * @return the total number of loadable resources in this counter
     */
    public int getTotalResourceCount() {
        return resources.isEmpty() ? 0 : resources.values().stream().reduce(0, Integer::sum);
    }

    /**
     * Computes the total resources in this counter.
     *
     * @return the total resources in this counter
     */
    public ResourceProfile getTotalResource() {
        return resources.entrySet().stream()
                .map(entry -> entry.getKey().getResourceProfile().multiply(entry.getValue()))
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    /**
     * Adds increment to this resource counter value and returns the resulting value.
     *
     * @param increment increment to add to this resource counter value
     * @return new ResourceCounter containing the result of the addition
     */
    public ResourceCounter add(ResourceCounter increment) {
        return internalAdd(increment.getLoadableResourcesWithCount());
    }

    /**
     * Adds the {@link ResourceProfile} with default empty loading into the counter.
     *
     * @param resourceProfile resource profile to add.
     * @return the new counter with the result.
     */
    @VisibleForTesting
    public ResourceCounter add(ResourceProfile resourceProfile) {
        return add(resourceProfile.toEmptyLoadsResourceProfile());
    }

    /**
     * Adds the {@link LoadableResourceProfile} into the counter.
     *
     * @param loadableResourceProfile loadable resource profile to add.
     * @return the new counter with the result.
     */
    public ResourceCounter add(LoadableResourceProfile loadableResourceProfile) {
        return add(loadableResourceProfile, 1);
    }

    /**
     * Adds the {@link LoadableResourceProfile} with incremental count into the counter.
     *
     * @param loadableResourceProfile loadable resource profile to add.
     * @return the new counter with the result.
     */
    public ResourceCounter add(LoadableResourceProfile loadableResourceProfile, int increment) {
        return internalAdd(
                withResource(loadableResourceProfile, increment).getLoadableResourcesWithCount());
    }

    private ResourceCounter internalAdd(
            Iterable<? extends Map.Entry<LoadableResourceProfile, Integer>> entries) {
        final Map<LoadableResourceProfile, Integer> newValues = new HashMap<>(resources);

        LOG.debug("internalAdd this pre {}", this);
        LOG.debug("internalAdd resourceCounter param entries {}, newValues {}", entries, newValues);

        for (Map.Entry<LoadableResourceProfile, Integer> resourceIncrement : entries) {
            final LoadableResourceProfile profileLoading = resourceIncrement.getKey();
            final int newCnt =
                    resources.getOrDefault(profileLoading, 0) + resourceIncrement.getValue();

            updateNewValue(newValues, profileLoading, newCnt);
        }
        LOG.debug("internalAdd result newValues: {}", newValues);

        return new ResourceCounter(newValues);
    }

    private void updateNewValue(
            Map<LoadableResourceProfile, Integer> newResources,
            final LoadableResourceProfile profileLoading,
            int newValue) {
        if (newValue > 0) {
            newResources.put(profileLoading, newValue);
        } else {
            newResources.remove(profileLoading);
        }
    }

    /**
     * Subtracts decrement from this resource counter value and returns the new value.
     *
     * @param decrement decrement to subtract from this resource counter
     * @return new ResourceCounter containing the new value
     */
    public ResourceCounter subtract(ResourceCounter decrement) {
        return internalSubtract(decrement.getLoadableResourcesWithCount());
    }

    /**
     * Subtracts the {@link LoadableResourceProfile} with 1 count out the counter.
     *
     * @param loadableResourceProfile loadable resource profile to subtract.
     * @return the new counter with the result.
     */
    public ResourceCounter subtract(LoadableResourceProfile loadableResourceProfile) {

        return subtract(loadableResourceProfile, 1);
    }

    /**
     * Subtracts the {@link ResourceProfile} whose weight is {@link LoadingWeight#EMPTY} with 1
     * count out the counter.
     *
     * @param resourceProfile resource profile to subtract.
     * @return the new counter with the result.
     */
    public ResourceCounter subtractIgnoreLoading(ResourceProfile resourceProfile) {

        return subtract(resourceProfile.toEmptyLoadsResourceProfile(), 1);
    }

    /**
     * Subtracts decrement from the count of the given resourceProfile whose contains default {@link
     * LoadingWeight#EMPTY} weight and returns the new value.
     *
     * @param resourceProfile resourceProfile from which to subtract decrement
     * @param decrement decrement is the number by which to decrease resourceProfile
     * @return new ResourceCounter containing the new value
     */
    @VisibleForTesting
    public ResourceCounter subtract(ResourceProfile resourceProfile, int decrement) {
        return internalSubtract(
                ResourceCounter.withResource(
                                resourceProfile.toEmptyLoadsResourceProfile(), decrement)
                        .getLoadableResourcesWithCount());
    }

    /**
     * Subtracts the {@link LoadableResourceProfile} with count out the counter.
     *
     * @param loadableResourceProfile loadable resource profile to subtract.
     * @return the new counter with the result.
     */
    public ResourceCounter subtract(
            LoadableResourceProfile loadableResourceProfile, int decrement) {
        return internalSubtract(
                ResourceCounter.withResource(loadableResourceProfile, decrement)
                        .getLoadableResourcesWithCount());
    }

    private ResourceCounter internalSubtract(
            Iterable<? extends Map.Entry<LoadableResourceProfile, Integer>> entries) {
        final Map<LoadableResourceProfile, Integer> newValues = new HashMap<>(resources);

        LOG.debug("internalSubtract this pre {}", this);
        LOG.debug(
                "internalSubtract resourceCounter param entries {}, newValues {}",
                entries,
                newValues);

        for (Map.Entry<LoadableResourceProfile, Integer> resourceDecrement : entries) {
            final LoadableResourceProfile profileLoading = resourceDecrement.getKey();
            final int newValue =
                    resources.getOrDefault(profileLoading, 0) - resourceDecrement.getValue();

            updateNewValue(newValues, profileLoading, newValue);
        }

        LOG.debug("internalSubtract result newValues {}", newValues);

        return new ResourceCounter(newValues);
    }

    /**
     * Gets the stored resources and their counts. The counts are guaranteed to be positive (> 0).
     *
     * @return collection of {@link LoadableResourceProfile} and count pairs
     */
    public Collection<Map.Entry<LoadableResourceProfile, Integer>> getLoadableResourcesWithCount() {
        return resources.entrySet();
    }

    /**
     * Checks whether resourceProfile is contained in this counter.
     *
     * @param resourceProfile resourceProfile to check whether it is contained
     * @return {@code true} if the counter has a positive count for the given resourceProfile;
     *     otherwise {@code false}
     */
    public boolean containsResource(ResourceProfile resourceProfile) {
        return resources.keySet().stream()
                .map(LoadableResourceProfile::getResourceProfile)
                .anyMatch(rp -> Objects.equals(resourceProfile, rp));
    }

    /**
     * Get the all loadable resource profiles.
     *
     * @return all loadable resource profiles.
     */
    public Set<LoadableResourceProfile> getLoadableResources() {
        return resources.keySet();
    }

    /**
     * Checks whether the resource counter is empty.
     *
     * @return {@code true} if the counter does not contain any counts; otherwise {@code false}
     */
    public boolean isEmpty() {
        return resources.isEmpty();
    }

    /**
     * Creates an empty resource counter.
     *
     * @return empty resource counter
     */
    public static ResourceCounter empty() {
        return new ResourceCounter(new HashMap<>());
    }

    /**
     * Creates a resource counter with the specified set of resources.
     *
     * @param loadableResourceProfile loadable resource profiles.
     * @param count count of the loadableResourceProfiles.
     * @return ResourceCounter which contains the specified set of resources
     */
    public static ResourceCounter withResource(
            LoadableResourceProfile loadableResourceProfile, int count) {
        return new ResourceCounter(
                new HashMap<LoadableResourceProfile, Integer>() {
                    {
                        put(loadableResourceProfile, count);
                    }
                });
    }

    /**
     * Creates a resource counter with the specified set of resources.
     *
     * @param resources a map that contains loadable resource profiles counter.
     * @return ResourceCounter which contains the specified set of resources
     */
    public static ResourceCounter withResource(Map<LoadableResourceProfile, Integer> resources) {
        return new ResourceCounter(resources);
    }

    /**
     * Creates a resource counter with the specified set of resources.
     *
     * @param resources a map that contains loadable resource profiles whoso weight are {@link
     *     LoadingWeight#EMPTY} counter.
     * @return ResourceCounter which contains the specified set of resources
     */
    @VisibleForTesting
    public static ResourceCounter withResources(Map<ResourceProfile, Integer> resources) {
        final Map<LoadableResourceProfile, Integer> map = new HashMap<>(resources.size());
        resources.forEach(
                (resourceProfile, integer) ->
                        map.put(resourceProfile.toEmptyLoadsResourceProfile(), integer));
        return new ResourceCounter(map);
    }

    /**
     * Creates a resource counter with the given resourceProfile with default {@link
     * LoadingWeight#EMPTY} weight and its count.
     *
     * @param resourceProfile resourceProfile for the given count
     * @param count count of the given resourceProfile
     * @return ResourceCounter which contains the specified resourceProfile with empty weight and
     *     its count
     */
    @VisibleForTesting
    public static ResourceCounter withResource(ResourceProfile resourceProfile, int count) {
        Preconditions.checkArgument(count >= 0);
        return count == 0
                ? empty()
                : ResourceCounter.withResource(
                        resourceProfile.toEmptyLoadsResourceProfile(), count);
    }

    /**
     * Creates a resource counter with the given resourceProfile with default {@link
     * LoadingWeight#EMPTY} weight and its count.
     *
     * @param resourceProfile resourceProfile for the given count
     * @return ResourceCounter which contains the specified resourceProfile with empty weight and
     *     its count
     */
    @VisibleForTesting
    public static ResourceCounter withResource(ResourceProfile resourceProfile) {
        return withResource(resourceProfile.toEmptyLoadsResourceProfile(), 1);
    }

    /**
     * Creates a resource counter with the given loadableResourceProfile and its count.
     *
     * @param loadableResourceProfile loadableResourceProfile for the given count
     * @return ResourceCounter which contains the specified loadableResourceProfile and its count
     */
    public static ResourceCounter withResource(LoadableResourceProfile loadableResourceProfile) {
        return ResourceCounter.withResource(loadableResourceProfile, 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceCounter that = (ResourceCounter) o;
        return Objects.equals(resources, that.resources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resources);
    }

    @Override
    public String toString() {
        return "ResourceCounter{" + "resources=" + resources + "}";
    }

    private static boolean isMeaningfulProfileLoading(
            Map.Entry<LoadableResourceProfile, Integer> entry) {
        return entry.getValue() != null && entry.getValue() > 0;
    }

    @Override
    @Nonnull
    public LoadingWeight getLoading() {
        return resources.entrySet().stream()
                .filter(ResourceCounter::isMeaningfulProfileLoading)
                .map(entry -> entry.getKey().getLoading().multiply(entry.getValue()))
                .reduce(LoadingWeight.EMPTY, LoadingWeight::merge);
    }
}
