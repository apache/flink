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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Counter for {@link ResourceProfile ResourceProfiles}. This class is immutable.
 *
 * <p>ResourceCounter contains a set of {@link ResourceProfile ResourceProfiles} and their
 * associated counts. The counts are always positive (> 0).
 */
public final class ResourceCounter implements WeightLoadable {

    private final Map<ResourceProfile, Integer> resources;

    Map<ResourceProfile, List<LoadingWeight>> loadingWeightsMap;

    private ResourceCounter(
            Map<ResourceProfile, Integer> resources,
            Map<ResourceProfile, List<LoadingWeight>> loadingWeightsMap) {
        this.resources = Collections.unmodifiableMap(resources);
        this.loadingWeightsMap = Preconditions.checkNotNull(loadingWeightsMap);
    }

    /**
     * Number of resources with the given {@link ResourceProfile}.
     *
     * @param resourceProfile resourceProfile for which to look up the count
     * @return number of resources with the given resourceProfile or {@code 0} if the resource
     *     profile does not exist
     */
    public int getResourceCount(ResourceProfile resourceProfile) {
        return resources.getOrDefault(resourceProfile, 0);
    }

    public Map<ResourceProfile, List<LoadingWeight>> getLoadingWeightsMap() {
        return Collections.unmodifiableMap(loadingWeightsMap);
    }

    /**
     * Computes the total number of resources in this counter.
     *
     * @return the total number of resources in this counter
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
                .map(entry -> entry.getKey().multiply(entry.getValue()))
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    /**
     * Adds increment to this resource counter value and returns the resulting value.
     *
     * @param increment increment to add to this resource counter value
     * @return new ResourceCounter containing the result of the addition
     */
    public ResourceCounter add(ResourceCounter increment) {
        return internalAdd(increment);
    }

    /**
     * Adds the given increment to this resource counter value and returns the resulting value.
     *
     * @param increment increment ot add to this resource counter value
     * @return new ResourceCounter containing the result of the addition
     */
    public ResourceCounter add(
            Map<ResourceProfile, Integer> increment,
            Map<ResourceProfile, List<LoadingWeight>> loadingWeightMap) {
        return internalAdd(ResourceCounter.withResources(increment, loadingWeightMap));
    }

    /**
     * Adds increment to the count of resourceProfile and returns the new value.
     *
     * @param resourceProfile resourceProfile to which to add increment
     * @param increment increment is the number by which to increase the resourceProfile
     * @return new ResourceCounter containing the result of the addition
     */
    public ResourceCounter add(
            ResourceProfile resourceProfile, int increment, List<LoadingWeight> loadingWeights) {
        return internalAdd(
                ResourceCounter.withResources(
                        Collections.singletonMap(resourceProfile, increment),
                        Collections.singletonMap(resourceProfile, loadingWeights)));
    }

    public ResourceCounter addWithEmptyLoadings(ResourceProfile resourceProfile, int increment) {
        return internalAdd(
                ResourceCounter.withResources(
                        Collections.singletonMap(resourceProfile, increment),
                        generateEmptyLoadingsFor(
                                Collections.singletonMap(resourceProfile, increment))));
    }

    private ResourceCounter internalAdd(ResourceCounter resourceCounter) {
        final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);
        final Map<ResourceProfile, List<LoadingWeight>> newLoadingMap =
                new HashMap<>(loadingWeightsMap);

        for (Map.Entry<ResourceProfile, Integer> resourceIncrement :
                resourceCounter.getResourcesWithCount()) {
            final ResourceProfile resourceProfile = resourceIncrement.getKey();
            final int newCnt =
                    resources.getOrDefault(resourceProfile, 0) + resourceIncrement.getValue();
            List<LoadingWeight> loadingToAdd = resourceCounter.getLoadingWeights(resourceProfile);

            updateNewValue(
                    newValues,
                    newLoadingMap,
                    resourceProfile,
                    newCnt,
                    loads -> loads.addAll(loadingToAdd));
        }

        return new ResourceCounter(newValues, newLoadingMap);
    }

    private void updateNewValue(
            Map<ResourceProfile, Integer> newResources,
            Map<ResourceProfile, List<LoadingWeight>> newLoadingsMap,
            ResourceProfile resourceProfile,
            int newValue,
            Consumer<List<LoadingWeight>> loadingOperation) {
        if (newValue > 0) {
            newResources.put(resourceProfile, newValue);
            List<LoadingWeight> loadings =
                    newLoadingsMap.computeIfAbsent(
                            resourceProfile, resourceProfile1 -> new ArrayList<>());
            loadingOperation.accept(loadings);
        } else {
            newResources.remove(resourceProfile);
            newLoadingsMap.remove(resourceProfile);
        }
    }

    /**
     * Subtracts decrement from this resource counter value and returns the new value.
     *
     * @param decrement decrement to subtract from this resource counter
     * @return new ResourceCounter containing the new value
     */
    public ResourceCounter subtract(ResourceCounter decrement) {
        return internalSubtract(decrement);
    }

    /**
     * Subtracts decrement from this resource counter value and returns the new value.
     *
     * @param decrement decrement to subtract from this resource counter
     * @return new ResourceCounter containing the new value
     */
    public ResourceCounter subtract(
            Map<ResourceProfile, Integer> decrement,
            Map<ResourceProfile, List<LoadingWeight>> loadingWeightsMap) {
        return internalSubtract(ResourceCounter.withResources(decrement, loadingWeightsMap));
    }

    /**
     * Subtracts decrement from the count of the given resourceProfile and returns the new value.
     *
     * @param resourceProfile resourceProfile from which to subtract decrement
     * @param decrement decrement is the number by which to decrease resourceProfile
     * @return new ResourceCounter containing the new value
     */
    public ResourceCounter subtract(
            ResourceProfile resourceProfile, int decrement, List<LoadingWeight> loadingWeights) {
        return internalSubtract(
                ResourceCounter.withResources(
                        Collections.singletonMap(resourceProfile, decrement),
                        Collections.singletonMap(resourceProfile, loadingWeights)));
    }

    public ResourceCounter subtractWithEmptyLoadings(
            ResourceProfile resourceProfile, int decrement) {
        return internalSubtract(
                ResourceCounter.withResources(
                        Collections.singletonMap(resourceProfile, decrement),
                        Collections.singletonMap(
                                resourceProfile, LoadingWeight.supplyEmptyLoadWeights(decrement))));
    }

    private ResourceCounter internalSubtract(ResourceCounter resourceCounter) {
        final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);
        final Map<ResourceProfile, List<LoadingWeight>> newLoadingMap =
                new HashMap<>(loadingWeightsMap);

        for (Map.Entry<ResourceProfile, Integer> resourceDecrement :
                resourceCounter.getResourcesWithCount()) {
            final ResourceProfile resourceProfile = resourceDecrement.getKey();
            final int newValue =
                    resources.getOrDefault(resourceProfile, 0) - resourceDecrement.getValue();
            List<LoadingWeight> loadingWeights = resourceCounter.getLoadingWeights(resourceProfile);

            updateNewValue(
                    newValues,
                    newLoadingMap,
                    resourceProfile,
                    newValue,
                    loads -> loads.removeAll(loadingWeights));
        }

        return new ResourceCounter(newValues, newLoadingMap);
    }

    /**
     * Gets the stored resources and their counts. The counts are guaranteed to be positive (> 0).
     *
     * @return collection of {@link ResourceProfile} and count pairs
     */
    public Collection<Map.Entry<ResourceProfile, Integer>> getResourcesWithCount() {
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
        return resources.containsKey(resourceProfile);
    }

    /**
     * Gets all stored {@link ResourceProfile ResourceProfiles}.
     *
     * @return collection of stored {@link ResourceProfile ResourceProfiles}
     */
    public Set<ResourceProfile> getResources() {
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
        return new ResourceCounter(Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Creates a resource counter with the specified set of resources.
     *
     * @param resources resources with which to initialize the resource counter
     * @return ResourceCounter which contains the specified set of resources
     */
    public static ResourceCounter withResources(
            Map<ResourceProfile, Integer> resources,
            Map<ResourceProfile, List<LoadingWeight>> loadingWeightsMap) {
        return new ResourceCounter(new HashMap<>(resources), new HashMap<>(loadingWeightsMap));
    }

    public static ResourceCounter withResources(Map<ResourceProfile, Integer> resources) {

        return new ResourceCounter(
                new HashMap<>(resources), new HashMap<>(generateEmptyLoadingsFor(resources)));
    }

    private static Map<ResourceProfile, List<LoadingWeight>> generateEmptyLoadingsFor(
            Map<ResourceProfile, Integer> resources) {
        final Map<ResourceProfile, List<LoadingWeight>> loadingMap = new HashMap<>();

        for (Map.Entry<ResourceProfile, Integer> entry : resources.entrySet()) {
            loadingMap.put(entry.getKey(), LoadingWeight.supplyEmptyLoadWeights(entry.getValue()));
        }
        return loadingMap;
    }

    /**
     * Creates a resource counter with the given resourceProfile and its count.
     *
     * @param resourceProfile resourceProfile for the given count
     * @param count count of the given resourceProfile
     * @return ResourceCounter which contains the specified resourceProfile and its count
     */
    public static ResourceCounter withResource(ResourceProfile resourceProfile, int count) {
        Preconditions.checkArgument(count >= 0);
        return count == 0
                ? empty()
                : new ResourceCounter(
                        Collections.singletonMap(resourceProfile, count),
                        Collections.singletonMap(
                                resourceProfile, LoadingWeight.supplyEmptyLoadWeights(count)));
    }

    public static ResourceCounter withResource(
            ResourceProfile resourceProfile, int count, List<LoadingWeight> loadingWeights) {
        Preconditions.checkArgument(count >= 0);
        Preconditions.checkArgument(count == loadingWeights.size());
        return count == 0
                ? empty()
                : new ResourceCounter(
                        Collections.singletonMap(resourceProfile, count),
                        Collections.singletonMap(resourceProfile, loadingWeights));
    }

    public static ResourceCounter withSingleResource(
            @Nonnull ResourceProfile resourceProfile, @Nonnull LoadingWeight loadingWeight) {
        return withResource(resourceProfile, 1, Collections.singletonList(loadingWeight));
    }

    public List<LoadingWeight> getLoadingWeights(ResourceProfile resourceProfile) {
        return loadingWeightsMap.getOrDefault(resourceProfile, new ArrayList<>());
    }

    public void addLoadingWeights(
            ResourceProfile resourceProfile, List<LoadingWeight> loadingWeights) {
        loadingWeightsMap.compute(
                resourceProfile,
                (resourceProfile1, oldWeights) -> {
                    if (oldWeights == null) {
                        return loadingWeights;
                    }
                    oldWeights.addAll(loadingWeights);
                    return oldWeights;
                });
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
        return "ResourceCounter{" + "resources=" + resources + '}';
    }

    @Override
    public LoadingWeight getLoading() {
        return loadingWeightsMap.values().stream()
                .flatMap(Collection::stream)
                .reduce(LoadingWeight.EMPTY, LoadingWeight::merge);
    }

    @Override
    public void setLoading(@Nonnull LoadingWeight loadingWeight) {
        throw new UnsupportedOperationException();
    }
}
