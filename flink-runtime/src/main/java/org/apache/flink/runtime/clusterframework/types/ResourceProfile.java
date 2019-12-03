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
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

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
public class ResourceProfile implements Serializable {

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
	public static final ResourceProfile ANY = newBuilder()
		.setCpuCores(Double.MAX_VALUE)
		.setTaskHeapMemory(MemorySize.MAX_VALUE)
		.setTaskOffHeapMemory(MemorySize.MAX_VALUE)
		.setManagedMemory(MemorySize.MAX_VALUE)
		.setShuffleMemory(MemorySize.MAX_VALUE)
		.build();

	/** A ResourceProfile describing zero resources. */
	public static final ResourceProfile ZERO = newBuilder().build();

	// ------------------------------------------------------------------------

	/** How many cpu cores are needed. Can be null only if it is unknown. */
	@Nullable
	private final Resource cpuCores;

	/** How much task heap memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize taskHeapMemory;

	/** How much task off-heap memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize taskOffHeapMemory;

	/** How much managed memory is needed. */
	@Nullable // can be null only for UNKNOWN
	private final MemorySize managedMemory;

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
	 * @param managedMemory The size of the managed memory.
	 * @param shuffleMemory The size of the shuffle memory.
	 * @param extendedResources The extended resources such as GPU and FPGA
	 */
	private ResourceProfile(
			final Resource cpuCores,
			final MemorySize taskHeapMemory,
			final MemorySize taskOffHeapMemory,
			final MemorySize managedMemory,
			final MemorySize shuffleMemory,
			final Map<String, Resource> extendedResources) {

		checkNotNull(cpuCores);
		checkArgument(cpuCores instanceof CPUResource, "cpuCores must be CPUResource");

		this.cpuCores = cpuCores;
		this.taskHeapMemory = checkNotNull(taskHeapMemory);
		this.taskOffHeapMemory = checkNotNull(taskOffHeapMemory);
		this.managedMemory = checkNotNull(managedMemory);
		this.shuffleMemory = checkNotNull(shuffleMemory);
		if (extendedResources != null) {
			this.extendedResources.putAll(extendedResources);
		}
	}

	/**
	 * Creates a special ResourceProfile with negative values, indicating resources are unspecified.
	 */
	private ResourceProfile() {
		this.cpuCores = null;
		this.taskHeapMemory = null;
		this.taskOffHeapMemory = null;
		this.managedMemory = null;
		this.shuffleMemory = null;
	}

	// ------------------------------------------------------------------------

	/**
	 * Get the cpu cores needed.
	 *
	 * @return The cpu cores, 1.0 means a full cpu thread
	 */
	public Resource getCpuCores() {
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
	 * Get the managed memory needed.
	 *
	 * @return The managed memory
	 */
	public MemorySize getManagedMemory() {
		throwUnsupportedOperationExecptionIfUnknown();
		return managedMemory;
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
		return taskHeapMemory.add(taskOffHeapMemory).add(managedMemory);
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
	public boolean isMatching(final ResourceProfile required) {
		checkNotNull(required, "Cannot check matching with null resources");

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

		if (cpuCores.getValue().compareTo(required.cpuCores.getValue()) >= 0 &&
			taskHeapMemory.compareTo(required.taskHeapMemory) >= 0 &&
			taskOffHeapMemory.compareTo(required.taskOffHeapMemory) >= 0 &&
			managedMemory.compareTo(required.managedMemory) >= 0 &&
			shuffleMemory.compareTo(required.shuffleMemory) >= 0) {

			for (Map.Entry<String, Resource> resource : required.extendedResources.entrySet()) {
				if (!extendedResources.containsKey(resource.getKey()) ||
					extendedResources.get(resource.getKey()).getValue().compareTo(resource.getValue().getValue()) < 0) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = Objects.hashCode(cpuCores);
		result = 31 * result + Objects.hashCode(taskHeapMemory);
		result = 31 * result + Objects.hashCode(taskOffHeapMemory);
		result = 31 * result + Objects.hashCode(managedMemory);
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
			return Objects.equals(this.cpuCores, that.cpuCores) &&
				Objects.equals(taskHeapMemory, that.taskHeapMemory) &&
				Objects.equals(taskOffHeapMemory, that.taskOffHeapMemory) &&
				Objects.equals(managedMemory, that.managedMemory) &&
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
	public ResourceProfile merge(final ResourceProfile other) {
		checkNotNull(other, "Cannot merge with null resources");

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
			cpuCores.merge(other.cpuCores),
			taskHeapMemory.add(other.taskHeapMemory),
			taskOffHeapMemory.add(other.taskOffHeapMemory),
			managedMemory.add(other.managedMemory),
			shuffleMemory.add(other.shuffleMemory),
			resultExtendedResource);
	}

	/**
	 * Subtracts another piece of resource profile from this one.
	 *
	 * @param other The other resource profile to subtract.
	 * @return The subtracted resource profile.
	 */
	public ResourceProfile subtract(final ResourceProfile other) {
		checkNotNull(other, "Cannot subtract with null resources");

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
				return resultResource.getValue().compareTo(BigDecimal.ZERO) == 0 ? null : resultResource;
			});
		});

		return new ResourceProfile(
			cpuCores.subtract(other.cpuCores),
			taskHeapMemory.subtract(other.taskHeapMemory),
			taskOffHeapMemory.subtract(other.taskOffHeapMemory),
			managedMemory.subtract(other.managedMemory),
			shuffleMemory.subtract(other.shuffleMemory),
			resultExtendedResource
		);
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
			resources.append(", ").append(resource.getKey()).append('=').append(resource.getValue().getValue());
		}
		return "ResourceProfile{" +
			"cpuCores=" + cpuCores.getValue() +
			", taskHeapMemory=" + taskHeapMemory +
			", taskOffHeapMemory=" + taskOffHeapMemory +
			", managedMemory=" + managedMemory +
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

		return newBuilder()
			.setCpuCores(resourceSpec.getCpuCores())
			.setTaskHeapMemory(resourceSpec.getTaskHeapMemory())
			.setTaskOffHeapMemory(resourceSpec.getTaskOffHeapMemory())
			.setManagedMemory(resourceSpec.getManagedMemory())
			.setShuffleMemory(networkMemory)
			.addExtendedResources(resourceSpec.getExtendedResources())
			.build();
	}

	@VisibleForTesting
	public static ResourceProfile fromResources(final double cpuCores, final int taskHeapMemoryMB) {
		return newBuilder()
			.setCpuCores(cpuCores)
			.setTaskHeapMemoryMB(taskHeapMemoryMB)
			.build();
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for the {@link ResourceProfile}.
	 */
	public static class Builder {

		private Resource cpuCores = new CPUResource(0.0);
		private MemorySize taskHeapMemory = MemorySize.ZERO;
		private MemorySize taskOffHeapMemory = MemorySize.ZERO;
		private MemorySize managedMemory = MemorySize.ZERO;
		private MemorySize shuffleMemory = MemorySize.ZERO;
		private Map<String, Resource> extendedResources = new HashMap<>();

		private Builder() {
		}

		public Builder setCpuCores(Resource cpuCores) {
			this.cpuCores = cpuCores;
			return this;
		}

		public Builder setCpuCores(double cpuCores) {
			this.cpuCores = new CPUResource(cpuCores);
			return this;
		}

		public Builder setTaskHeapMemory(MemorySize taskHeapMemory) {
			this.taskHeapMemory = taskHeapMemory;
			return this;
		}

		public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
			this.taskHeapMemory = MemorySize.parse(taskHeapMemoryMB + "m");
			return this;
		}

		public Builder setTaskOffHeapMemory(MemorySize taskOffHeapMemory) {
			this.taskOffHeapMemory = taskOffHeapMemory;
			return this;
		}

		public Builder setTaskOffHeapMemoryMB(int taskOffHeapMemoryMB) {
			this.taskOffHeapMemory = MemorySize.parse(taskOffHeapMemoryMB + "m");
			return this;
		}

		public Builder setManagedMemory(MemorySize managedMemory) {
			this.managedMemory = managedMemory;
			return this;
		}

		public Builder setManagedMemoryMB(int managedMemoryMB) {
			this.managedMemory = MemorySize.parse(managedMemoryMB + "m");
			return this;
		}

		public Builder setShuffleMemory(MemorySize shuffleMemory) {
			this.shuffleMemory = shuffleMemory;
			return this;
		}

		public Builder setShuffleMemoryMB(int shuffleMemoryMB) {
			this.shuffleMemory = MemorySize.parse(shuffleMemoryMB + "m");
			return this;
		}

		public Builder addExtendedResource(String name, Resource extendedResource) {
			this.extendedResources.put(name, extendedResource);
			return this;
		}

		public Builder addExtendedResources(Map<String, Resource> extendedResources) {
			if (extendedResources != null) {
				this.extendedResources.putAll(extendedResources);
			}
			return this;
		}

		public ResourceProfile build() {
			return new ResourceProfile(
				cpuCores,
				taskHeapMemory,
				taskOffHeapMemory,
				managedMemory,
				shuffleMemory,
				extendedResources);
		}
	}
}
