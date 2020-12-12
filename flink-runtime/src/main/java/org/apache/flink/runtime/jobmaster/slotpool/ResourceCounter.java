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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Counter for {@link ResourceProfile ResourceProfiles}. This class is immutable.
 *
 * <p>ResourceCounter contains a set of {@link ResourceProfile ResourceProfiles} and their
 * associated counts. The counts are always positive (> 0).
 */
final class ResourceCounter {

	private final Map<ResourceProfile, Integer> resources;

	private ResourceCounter(Map<ResourceProfile, Integer> resources) {
		this.resources = Collections.unmodifiableMap(resources);
	}

	/**
	 * Number of resources with the given {@link ResourceProfile}.
	 *
	 * @param resourceProfile resourceProfile for which to look up the count
	 * @return number of resources with the given resourceProfile or {@code 0}
	 * if the resource profile does not exist
	 */
	public int getResourceCount(ResourceProfile resourceProfile) {
		return resources.getOrDefault(resourceProfile, 0);
	}

	/**
	 * Adds increment to this resource counter value and returns the resulting value.
	 *
	 * @param increment increment to add to this resource counter value
	 * @return new ResourceCounter containing the result of the addition
	 */
	public ResourceCounter add(ResourceCounter increment) {
		return internalAdd(increment.getResourcesWithCount());
	}

	/**
	 * Adds the given increment to this resource counter value and returns the resulting value.
	 *
	 * @param increment increment ot add to this resource counter value
	 * @return new ResourceCounter containing the result of the addition
	 */
	public ResourceCounter add(Map<ResourceProfile, Integer> increment) {
		return internalAdd(increment.entrySet());
	}

	/**
	 * Adds increment to the count of resourceProfile and returns the new value.
	 *
	 * @param resourceProfile resourceProfile to which to add increment
	 * @param increment increment is the number by which to increase the resourceProfile
	 * @return new ResourceCounter containing the result of the addition
	 */
	public ResourceCounter add(ResourceProfile resourceProfile, int increment) {
		final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);
		final int newValue = resources.getOrDefault(resourceProfile, 0) + increment;

		updateNewValue(newValues, resourceProfile, newValue);

		return new ResourceCounter(newValues);
	}

	private ResourceCounter internalAdd(Iterable<? extends Map.Entry<ResourceProfile, Integer>> entries) {
		final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);

		for (Map.Entry<ResourceProfile, Integer> resourceIncrement : entries) {
			final ResourceProfile resourceProfile = resourceIncrement.getKey();

			final int newValue = resources.getOrDefault(resourceProfile, 0) + resourceIncrement.getValue();

			updateNewValue(newValues, resourceProfile, newValue);
		}

		return new ResourceCounter(newValues);
	}

	private void updateNewValue(Map<ResourceProfile, Integer> newResources, ResourceProfile resourceProfile, int newValue) {
		if (newValue > 0) {
			newResources.put(resourceProfile, newValue);
		} else {
			newResources.remove(resourceProfile);
		}
	}

	/**
	 * Subtracts decrement from this resource counter value and returns the new value.
	 *
	 * @param decrement decrement to subtract from this resource counter
	 * @return new ResourceCounter containing the new value
	 */
	public ResourceCounter subtract(ResourceCounter decrement) {
		return internalSubtract(decrement.getResourcesWithCount());
	}

	/**
	 * Subtracts decrement from this resource counter value and returns the new value.
	 *
	 * @param decrement decrement to subtract from this resource counter
	 * @return new ResourceCounter containing the new value
	 */
	public ResourceCounter subtract(Map<ResourceProfile, Integer> decrement) {
		return internalSubtract(decrement.entrySet());
	}

	/**
	 * Subtracts decrement from the count of the given resourceProfile and returns the new value.
	 *
	 * @param resourceProfile resourceProfile from which to subtract decrement
	 * @param decrement decrement is the number by which to decrease resourceProfile
	 * @return new ResourceCounter containing the new value
	 */
	public ResourceCounter subtract(ResourceProfile resourceProfile, int decrement) {
		final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);
		final int newValue = resources.getOrDefault(resourceProfile, 0) - decrement;

		updateNewValue(newValues, resourceProfile, newValue);

		return new ResourceCounter(newValues);
	}

	private ResourceCounter internalSubtract(Iterable<? extends Map.Entry<ResourceProfile, Integer>> entries) {
		final Map<ResourceProfile, Integer> newValues = new HashMap<>(resources);

		for (Map.Entry<ResourceProfile, Integer> resourceDecrement : entries) {
			final ResourceProfile resourceProfile = resourceDecrement.getKey();
			final int newValue = resources.getOrDefault(resourceProfile, 0) - resourceDecrement.getValue();

			updateNewValue(newValues, resourceProfile, newValue);
		}

		return new ResourceCounter(newValues);
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
	 * otherwise {@code false}
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
	 * @return {@code true} if the counter does not contain any counts;
	 * otherwise {@code false}
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
		return new ResourceCounter(Collections.emptyMap());
	}

	/**
	 * Creates a resource counter with the specified set of resources.
	 *
	 * @param resources resources with which to initialize the resource counter
	 * @return ResourceCounter which contains the specified set of resources
	 */
	public static ResourceCounter withResources(Map<ResourceProfile, Integer> resources) {
		return new ResourceCounter(new HashMap<>(resources));
	}

	/**
	 * Creates a resource counter with the given resourceProfile and its count.
	 *
	 * @param resourceProfile resourceProfile for the given count
	 * @param count count of the given resourceProfile
	 * @return ResourceCounter which contains the specified resourceProfile and its count
	 */
	public static ResourceCounter withResource(ResourceProfile resourceProfile, int count) {
		return new ResourceCounter(Collections.singletonMap(resourceProfile, count));
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
		return "ResourceCounter{" +
			"resources=" + resources +
			'}';
	}
}
