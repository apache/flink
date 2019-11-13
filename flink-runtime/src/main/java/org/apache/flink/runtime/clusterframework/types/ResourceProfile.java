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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describe the immutable resource profile of the slot, either when requiring or offering it. The profile can be
 * checked whether it can match another profile's requirement, and furthermore we may calculate a matching
 * score to decide which profile we should choose when we have lots of candidate slots.
 * It should be generated from {@link ResourceSpec} with the input and output memory calculated in JobMaster.
 *
 * <p>Resource Profiles have a total ordering, defined by comparing these fields in sequence:
 * <ol>
 *     <li>Memory Size</li>
 *     <li>CPU cores</li>
 *     <li>Extended resources</li>
 * </ol>
 * The extended resources are compared ordered by the resource names.
 */
public class ResourceProfile implements Serializable, Comparable<ResourceProfile> {

	private static final long serialVersionUID = 1L;

	/**
	 * A ResourceProfile that indicates an unknown resource requirement.
	 * This is mainly used for describing resource requirements that the exact amount of resource needed is not specified.
	 * It can also be used for describing remaining resource of a multi task slot that contains tasks with unknown resource requirements.
	 * It should not be used for describing total resource of a task executor / slot, which should always be specific.
	 */
	public static final ResourceProfile UNKNOWN = new ResourceProfile();

	/**
	 * A ResourceProfile that indicates infinite resource that matches any resource requirement, for testability purpose only.
	 */
	@VisibleForTesting
	public static final ResourceProfile ANY = new ResourceProfile(Double.MAX_VALUE, MemorySize.MAX_VALUE, MemorySize.MAX_VALUE, MemorySize.MAX_VALUE, MemorySize.MAX_VALUE, MemorySize.MAX_VALUE, Collections.emptyMap());

	/** A ResourceProfile describing zero resources. */
	public static final ResourceProfile ZERO = new ResourceProfile(0, MemorySize.ZERO);

	// ------------------------------------------------------------------------

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1. */
	private final double cpuCores;

	/** How much task heap memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize taskHeapMemory;

	/** How much task off-heap memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize taskOffHeapMemory;

	/** How much on-heap managed memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize onHeapManagedMemory;

	/** How much off-heap managed memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize offHeapManagedMemory;

	/** How much shuffle memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize shuffleMemory;

	/** A extensible field for user specified resources from {@link ResourceSpec}. */
	private final Map<String, Resource> extendedResources = new HashMap<>(1);

	// ------------------------------------------------------------------------

	/**
	 * Creates a new ResourceProfile.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param taskHeapMemory The size of the task heap memory.
	 * @param taskOffHeapMemory The size of the task off-heap memory.
	 * @param onHeapManagedMemory The size of the on-heap managed memory.
	 * @param offHeapManagedMemory The size of the off-heap managed memory.
	 * @param shuffleMemory The size of the shuffle memory.
	 * @param extendedResources The extended resources such as GPU and FPGA
	 */
	public ResourceProfile(
			double cpuCores,
			MemorySize taskHeapMemory,
			MemorySize taskOffHeapMemory,
			MemorySize onHeapManagedMemory,
			MemorySize offHeapManagedMemory,
			MemorySize shuffleMemory,
			Map<String, Resource> extendedResources) {
		this.cpuCores = cpuCores;
		this.taskHeapMemory = taskHeapMemory;
		this.taskOffHeapMemory = taskOffHeapMemory;
		this.onHeapManagedMemory = onHeapManagedMemory;
		this.offHeapManagedMemory = offHeapManagedMemory;
		this.shuffleMemory = shuffleMemory;
		if (extendedResources != null) {
			this.extendedResources.putAll(extendedResources);
		}
	}

	@VisibleForTesting
	public ResourceProfile(
		double cpuCores,
		int taskHeapMemoryMB,
		int taskOffHeapMemoryMB,
		int onHeapManagedMemoryMB,
		int offHeapManagedMemoryMB,
		int shuffleMemoryMB,
		Map<String, Resource> extendedResources) {

		this(
			cpuCores,
			MemorySize.parse(taskHeapMemoryMB + "m"),
			MemorySize.parse(taskOffHeapMemoryMB + "m"),
			MemorySize.parse(onHeapManagedMemoryMB + "m"),
			MemorySize.parse(offHeapManagedMemoryMB + "m"),
			MemorySize.parse(shuffleMemoryMB + "m"),
			extendedResources);
	}

	@VisibleForTesting
	public ResourceProfile(double cpuCores, MemorySize taskHeapMemory) {
		this(cpuCores, taskHeapMemory, MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO, Collections.emptyMap());
	}

	@VisibleForTesting
	public ResourceProfile(double cpuCores, int taskHeapMemoryMB) {
		this(cpuCores, MemorySize.parse(taskHeapMemoryMB + "m"), MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO, MemorySize.ZERO, Collections.emptyMap());
	}

	/**
	 * Creates a special ResourceProfile with negative values, indicating resources are unspecified.
	 */
	private ResourceProfile() {
		this.cpuCores = -1.0;
		this.taskHeapMemory = null;
		this.taskOffHeapMemory = null;
		this.onHeapManagedMemory = null;
		this.offHeapManagedMemory = null;
		this.shuffleMemory = null;
	}

	// ------------------------------------------------------------------------

	/**
	 * Get the cpu cores needed.
	 *
	 * @return The cpu cores, 1.0 means a full cpu thread
	 */
	public double getCpuCores() {
		throwUnsupportedOperationExecptionIfUnknown();
		return cpuCores;
	}

	/**
	 * Get the task heap memory needed.
	 *
	 * @return The task heap memory
	 */
	public MemorySize getTaskHeapMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return taskHeapMemory;
	}

	/**
	 * Get the task off-heap memory needed.
	 *
	 * @return The task off-heap memory
	 */
	public MemorySize getTaskOffHeapMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return taskOffHeapMemory;
	}

	/**
	 * Get the on-heap managed memory needed.
	 *
	 * @return The on-heap managed memory
	 */
	public MemorySize getOnHeapManagedMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return onHeapManagedMemory;
	}

	/**off
	 * Get the off-heap managed memory needed.
	 *
	 * @return The off-heap managed memory
	 */
	public MemorySize getOffHeapManagedMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return offHeapManagedMemory;
	}

	/**
	 * Get the shuffle memory needed.
	 *
	 * @return The shuffle memory
	 */
	public MemorySize getShuffleMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return shuffleMemory;
	}

	/**
	 * Get the total memory needed.
	 *
	 * @return The total memory
	 */
	public MemorySize getTotalMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return getOperatorsMemory().add(shuffleMemory);
	}

	/**
	 * Get the memory the operators needed.
	 *
	 * @return The operator memory
	 */
	public MemorySize getOperatorsMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return taskHeapMemory.add(taskOffHeapMemory).add(onHeapManagedMemory).add(offHeapManagedMemory);
	}

	/**
	 * Get the extended resources.
	 *
	 * @return The extended resources
	 */
	public Map<String, Resource> getExtendedResources() {
		throwUnsupportedOperationExecptionIfUnknown();
		return Collections.unmodifiableMap(extendedResources);
	}

	private void throwUnsupportedOperationExecptionIfUnknown() {
		if (this.equals(UNKNOWN)) {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Check whether required resource profile can be matched.
	 *
	 * @param required the required resource profile
	 * @return true if the requirement is matched, otherwise false
	 */
	public boolean isMatching(ResourceProfile required) {

		if (this.equals(ANY)) {
			return true;
		}

		if (this.equals(required)) {
			return true;
		}

		if (this.equals(UNKNOWN)) {
			return false;
		}

		if (required.equals(UNKNOWN)) {
			return true;
		}

		if (cpuCores >= required.getCpuCores() &&
			taskHeapMemory.getBytes() >= required.taskHeapMemory.getBytes() &&
			taskOffHeapMemory.getBytes() >= required.taskOffHeapMemory.getBytes() &&
			onHeapManagedMemory.getBytes() >= required.onHeapManagedMemory.getBytes() &&
			offHeapManagedMemory.getBytes() >= required.offHeapManagedMemory.getBytes() &&
			shuffleMemory.getBytes() >= required.shuffleMemory.getBytes()) {
			for (Map.Entry<String, Resource> resource : required.extendedResources.entrySet()) {
				if (!extendedResources.containsKey(resource.getKey()) ||
						!extendedResources.get(resource.getKey()).getResourceAggregateType().equals(resource.getValue().getResourceAggregateType()) ||
						extendedResources.get(resource.getKey()).getValue() < resource.getValue().getValue()) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public int compareTo(@Nonnull ResourceProfile other) {
		if (this == other) {
			return 0;
		} else if (this.equals(UNKNOWN)) {
			return -1;
		} else if (other.equals(UNKNOWN)) {
			return 1;
		}

		int cmp = this.getTotalMemory().compareTo(other.getTotalMemory());
		if (cmp == 0) {
			cmp = Double.compare(this.cpuCores, other.cpuCores);
		}
		if (cmp == 0) {
			Iterator<Map.Entry<String, Resource>> thisIterator = extendedResources.entrySet().iterator();
			Iterator<Map.Entry<String, Resource>> otherIterator = other.extendedResources.entrySet().iterator();
			while (thisIterator.hasNext() && otherIterator.hasNext()) {
				Map.Entry<String, Resource> thisResource = thisIterator.next();
				Map.Entry<String, Resource> otherResource = otherIterator.next();
				if ((cmp = otherResource.getKey().compareTo(thisResource.getKey())) != 0) {
					return cmp;
				}
				if (!otherResource.getValue().getResourceAggregateType().equals(thisResource.getValue().getResourceAggregateType())) {
					return 1;
				}
				if ((cmp = Double.compare(thisResource.getValue().getValue(), otherResource.getValue().getValue())) != 0) {
					return cmp;
				}
			}
			if (thisIterator.hasNext()) {
				return 1;
			}
			if (otherIterator.hasNext()) {
				return -1;
			}
		}
		return cmp;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final long cpuBits =  Double.doubleToLongBits(cpuCores);
		int result = (int) (cpuBits ^ (cpuBits >>> 32));
		result = 31 * result + Objects.hashCode(taskHeapMemory);
		result = 31 * result + Objects.hashCode(taskOffHeapMemory);
		result = 31 * result + Objects.hashCode(onHeapManagedMemory);
		result = 31 * result + Objects.hashCode(offHeapManagedMemory);
		result = 31 * result + Objects.hashCode(shuffleMemory);
		result = 31 * result + extendedResources.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == ResourceProfile.class) {
			ResourceProfile that = (ResourceProfile) obj;
			return this.cpuCores == that.cpuCores &&
				Objects.equals(taskHeapMemory, that.taskHeapMemory) &&
				Objects.equals(taskOffHeapMemory, that.taskOffHeapMemory) &&
				Objects.equals(onHeapManagedMemory, that.onHeapManagedMemory) &&
				Objects.equals(offHeapManagedMemory, that.offHeapManagedMemory) &&
				Objects.equals(shuffleMemory, that.shuffleMemory) &&
				Objects.equals(extendedResources, that.extendedResources);
		}
		return false;
	}

	/**
	 * Calculates the sum of two resource profiles.
	 *
	 * @param other The other resource profile to add.
	 * @return The merged resource profile.
	 */
	@Nonnull
	public ResourceProfile merge(@Nonnull ResourceProfile other) {
		if (equals(ANY) || other.equals(ANY)) {
			return ANY;
		}

		if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
			return UNKNOWN;
		}

		Map<String, Resource> resultExtendedResource = new HashMap<>(extendedResources);

		other.extendedResources.forEach((String name, Resource resource) -> {
			resultExtendedResource.compute(name, (ignored, oldResource) ->
				oldResource == null ? resource : oldResource.merge(resource));
		});

		return new ResourceProfile(
			addNonNegativeDoublesConsideringOverflow(cpuCores, other.cpuCores),
			taskHeapMemory.add(other.taskHeapMemory),
			taskOffHeapMemory.add(other.taskOffHeapMemory),
			onHeapManagedMemory.add(other.onHeapManagedMemory),
			offHeapManagedMemory.add(other.offHeapManagedMemory),
			shuffleMemory.add(other.shuffleMemory),
			resultExtendedResource);
	}

	/**
	 * Subtracts another piece of resource profile from this one.
	 *
	 * @param other The other resource profile to subtract.
	 * @return The subtracted resource profile.
	 */
	public ResourceProfile subtract(ResourceProfile other) {
		if (equals(ANY) || other.equals(ANY)) {
			return ANY;
		}

		if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
			return UNKNOWN;
		}

		checkArgument(isMatching(other), "Try to subtract an unmatched resource profile from this one.");

		Map<String, Resource> resultExtendedResource = new HashMap<>(extendedResources);

		other.extendedResources.forEach((String name, Resource resource) -> {
			resultExtendedResource.compute(name, (ignored, oldResource) -> {
				Resource resultResource = oldResource.subtract(resource);
				return resultResource.getValue() == 0 ? null : resultResource;
			});
		});

		return new ResourceProfile(
			subtractDoublesConsideringInf(cpuCores, other.cpuCores),
			taskHeapMemory.subtract(other.taskHeapMemory),
			taskOffHeapMemory.subtract(other.taskOffHeapMemory),
			onHeapManagedMemory.subtract(other.onHeapManagedMemory),
			offHeapManagedMemory.subtract(other.offHeapManagedMemory),
			shuffleMemory.subtract(other.shuffleMemory),
			resultExtendedResource
		);
	}

	private double addNonNegativeDoublesConsideringOverflow(double first, double second) {
		double result = first + second;

		if (Double.isInfinite(result)) {
			throw new ArithmeticException("double overflow");
		}

		return result;
	}

	private double subtractDoublesConsideringInf(double first, double second) {
		double result = first - second;

		if (Double.isInfinite(result)) {
			throw new ArithmeticException("double overflow");
		}

		return result;
	}

	@Override
	public String toString() {
		if (this.equals(UNKNOWN)) {
			return "ResourceProfile{UNKNOWN}";
		}

		if (this.equals(ANY)) {
			return "ResourceProfile{ANY}";
		}

		final StringBuilder resources = new StringBuilder(extendedResources.size() * 10);
		for (Map.Entry<String, Resource> resource : extendedResources.entrySet()) {
			resources.append(", ").append(resource.getKey()).append('=').append(resource.getValue());
		}
		return "ResourceProfile{" +
			"cpuCores=" + cpuCores +
			", taskHeapMemory=" + taskHeapMemory +
			", taskOffHeapMemory=" + taskOffHeapMemory +
			", onHeapManagedMemory=" + onHeapManagedMemory +
			", offHeapManagedMemory=" + offHeapManagedMemory +
			", shuffleMemory=" + shuffleMemory + resources +
			'}';
	}

	// ------------------------------------------------------------------------
	//  serialization
	// ------------------------------------------------------------------------

	private Object readResolve() {
		// try to preserve the singleton property for UNKNOWN and ANY

		if (this.equals(UNKNOWN)) {
			return UNKNOWN;
		}

		if (this.equals(ANY)) {
			return ANY;
		}

		return this;
	}

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	@VisibleForTesting
	static ResourceProfile fromResourceSpec(ResourceSpec resourceSpec) {
		return fromResourceSpec(resourceSpec, MemorySize.ZERO);
	}

	public static ResourceProfile fromResourceSpec(ResourceSpec resourceSpec, MemorySize networkMemory) {
		if (ResourceSpec.UNKNOWN.equals(resourceSpec)) {
			return UNKNOWN;
		}

		Map<String, Resource> copiedExtendedResources = new HashMap<>(resourceSpec.getExtendedResources());

		return new ResourceProfile(
			resourceSpec.getCpuCores(),
			resourceSpec.getTaskHeapMemory(),
			resourceSpec.getTaskOffHeapMemory(),
			resourceSpec.getOnHeapManagedMemory(),
			resourceSpec.getOffHeapManagedMemory(),
			networkMemory,
			copiedExtendedResources);
	}
}
