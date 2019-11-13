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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.resources.GPUResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.MemorySize;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describe the different resource factors of the operator with UDF.
 *
 * <p>Resource provides {@link #merge(ResourceSpec)} method for chained operators when generating job graph.
 *
 * <p>Resource provides {@link #lessThanOrEqual(ResourceSpec)} method to compare these fields in sequence:
 * <ol>
 *     <li>CPU cores</li>
 *     <li>Task Heap Memory</li>
 *     <li>Task Off-Heap Memory</li>
 *     <li>On-Heap Managed Memory</li>
 *     <li>Off-Heap Managed Memory</li>
 *     <li>Extended resources</li>
 * </ol>
 */
@Internal
public final class ResourceSpec implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * A ResourceSpec that indicates an unknown set of resources.
	 */
	public static final ResourceSpec UNKNOWN = new ResourceSpec();

	/**
	 * The default ResourceSpec used for operators and transformation functions.
	 * Currently equal to {@link #UNKNOWN}.
	 */
	public static final ResourceSpec DEFAULT = UNKNOWN;

	public static final String GPU_NAME = "GPU";

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

	private final Map<String, Resource> extendedResources = new HashMap<>(1);

	private ResourceSpec(
		double cpuCores,
		MemorySize taskHeapMemory,
		MemorySize taskOffHeapMemory,
		MemorySize onHeapManagedMemory,
		MemorySize offHeapManagedMemory,
		Resource... extendedResources) {

		checkArgument(cpuCores >= 0, "The cpu cores of the resource spec should not be negative.");

		this.cpuCores = cpuCores;
		this.taskHeapMemory = checkNotNull(taskHeapMemory);
		this.taskOffHeapMemory = checkNotNull(taskOffHeapMemory);
		this.onHeapManagedMemory = checkNotNull(onHeapManagedMemory);
		this.offHeapManagedMemory = checkNotNull(offHeapManagedMemory);
		for (Resource resource : extendedResources) {
			if (resource != null) {
				this.extendedResources.put(resource.getName(), resource);
			}
		}
	}

	/**
	 * Creates a new ResourceSpec with all fields unknown.
	 */
	private ResourceSpec() {
		this.cpuCores = -1;
		this.taskHeapMemory = null;
		this.taskOffHeapMemory = null;
		this.onHeapManagedMemory = null;
		this.offHeapManagedMemory = null;
	}

	/**
	 * Used by system internally to merge the other resources of chained operators
	 * when generating the job graph.
	 *
	 * @param other Reference to resource to merge in.
	 * @return The new resource with merged values.
	 */
	public ResourceSpec merge(ResourceSpec other) {
		if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
			return UNKNOWN;
		}

		ResourceSpec target = new ResourceSpec(
			this.cpuCores + other.cpuCores,
			this.taskHeapMemory.add(other.taskHeapMemory),
			this.taskOffHeapMemory.add(other.taskOffHeapMemory),
			this.onHeapManagedMemory.add(other.onHeapManagedMemory),
			this.offHeapManagedMemory.add(other.offHeapManagedMemory));
		target.extendedResources.putAll(extendedResources);
		for (Resource resource : other.extendedResources.values()) {
			target.extendedResources.merge(resource.getName(), resource, (v1, v2) -> v1.merge(v2));
		}
		return target;
	}

	public double getCpuCores() {
		throwUnsupportedOperationExceptionIfUnknown();
		return this.cpuCores;
	}

	public MemorySize getTaskHeapMemory() {
		throwUnsupportedOperationExceptionIfUnknown();
		return this.taskHeapMemory;
	}

	public MemorySize getTaskOffHeapMemory() {
		throwUnsupportedOperationExceptionIfUnknown();
		return taskOffHeapMemory;
	}

	public MemorySize getOnHeapManagedMemory() {
		throwUnsupportedOperationExceptionIfUnknown();
		return onHeapManagedMemory;
	}

	public MemorySize getOffHeapManagedMemory() {
		throwUnsupportedOperationExceptionIfUnknown();
		return offHeapManagedMemory;
	}

	public double getGPUResource() {
		throwUnsupportedOperationExceptionIfUnknown();
		Resource gpuResource = extendedResources.get(GPU_NAME);
		if (gpuResource != null) {
			return gpuResource.getValue();
		}

		return 0.0;
	}

	public Map<String, Resource> getExtendedResources() {
		throwUnsupportedOperationExceptionIfUnknown();
		return extendedResources;
	}

	private void throwUnsupportedOperationExceptionIfUnknown() {
		if (this.equals(UNKNOWN)) {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Check whether all the field values are valid.
	 *
	 * @return True if all the values are equal or greater than 0, otherwise false.
	 */
	public boolean isValid() {
		if (this.equals(UNKNOWN)) {
			return true;
		}
		if (this.cpuCores < 0) {
			return false;
		}
		for (Resource resource : extendedResources.values()) {
			if (resource.getValue() < 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Checks the current resource less than or equal with the other resource by comparing
	 * all the fields in the resource.
	 *
	 * @param other The resource to compare
	 * @return True if current resource is less than or equal with the other resource, otherwise return false.
	 */
	public boolean lessThanOrEqual(@Nonnull ResourceSpec other) {
		if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
			throw new IllegalArgumentException("UNKNOWN ResourceSpecs cannot be numerically compared.");
		}
		int cmp1 = Double.compare(this.cpuCores, other.cpuCores);
		int cmp2 = this.taskHeapMemory.compareTo(other.taskHeapMemory);
		int cmp3 = this.taskOffHeapMemory.compareTo(other.taskOffHeapMemory);
		int cmp4 = this.onHeapManagedMemory.compareTo(other.onHeapManagedMemory);
		int cmp5 = this.offHeapManagedMemory.compareTo(other.offHeapManagedMemory);
		if (cmp1 <= 0 && cmp2 <= 0 && cmp3 <= 0 && cmp4 <= 0 && cmp5 <= 0) {
			for (Resource resource : extendedResources.values()) {
				if (!other.extendedResources.containsKey(resource.getName()) ||
					other.extendedResources.get(resource.getName()).getResourceAggregateType() != resource.getResourceAggregateType() ||
						other.extendedResources.get(resource.getName()).getValue() < resource.getValue()) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == ResourceSpec.class) {
			ResourceSpec that = (ResourceSpec) obj;
			return this.cpuCores == that.cpuCores &&
				Objects.equals(this.taskHeapMemory, that.taskHeapMemory) &&
				Objects.equals(this.taskOffHeapMemory, that.taskOffHeapMemory) &&
				Objects.equals(this.onHeapManagedMemory, that.onHeapManagedMemory) &&
				Objects.equals(this.offHeapManagedMemory, that.offHeapManagedMemory) &&
				Objects.equals(extendedResources, that.extendedResources);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		final long cpuBits =  Double.doubleToLongBits(cpuCores);
		int result = (int) (cpuBits ^ (cpuBits >>> 32));
		result = 31 * result + Objects.hashCode(taskHeapMemory);
		result = 31 * result + Objects.hashCode(taskOffHeapMemory);
		result = 31 * result + Objects.hashCode(onHeapManagedMemory);
		result = 31 * result + Objects.hashCode(offHeapManagedMemory);
		result = 31 * result + extendedResources.hashCode();
		return result;
	}

	@Override
	public String toString() {
		if (this.equals(UNKNOWN)) {
			return "ResourceSpec{UNKNOWN}";
		}

		StringBuilder extend = new StringBuilder();
		for (Resource resource : extendedResources.values()) {
			extend.append(", ").append(resource.getName()).append("=").append(resource.getValue());
		}
		return "ResourceSpec{" +
			"cpuCores=" + cpuCores +
			", taskHeapMemory=" + taskHeapMemory +
			", taskOffHeapMemory=" + taskOffHeapMemory +
			", onHeapManagedMemory=" + onHeapManagedMemory +
			", offHeapManagedMemory=" + offHeapManagedMemory +
			'}';
	}

	// ------------------------------------------------------------------------
	//  serialization
	// ------------------------------------------------------------------------

	private Object readResolve() {
		// try to preserve the singleton property for UNKNOWN
		return this.equals(UNKNOWN) ? UNKNOWN : this;
	}

	// ------------------------------------------------------------------------
	//  builder
	// ------------------------------------------------------------------------

	public static Builder newBuilder(double cpuCores, MemorySize taskHeapMemory) {
		return new Builder(cpuCores, taskHeapMemory);
	}

	public static Builder newBuilder(double cpuCores, int taskHeapMemoryMB) {
		return newBuilder(cpuCores, MemorySize.parse(taskHeapMemoryMB + "m"));
	}

	/**
	 * Builder for the {@link ResourceSpec}.
	 */
	public static class Builder {

		private double cpuCores;
		private MemorySize taskHeapMemory;
		private MemorySize taskOffHeapMemory = MemorySize.ZERO;
		private MemorySize onHeapManagedMemory = MemorySize.ZERO;
		private MemorySize offHeapManagedMemory = MemorySize.ZERO;
		private GPUResource gpuResource;

		private Builder(double cpuCores, MemorySize taskHeapMemory) {
			this.cpuCores = cpuCores;
			this.taskHeapMemory = taskHeapMemory;
		}

		public Builder setCpuCores(double cpuCores) {
			this.cpuCores = cpuCores;
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

		public Builder setOffTaskHeapMemoryMB(int taskOffHeapMemoryMB) {
			this.taskOffHeapMemory = MemorySize.parse(taskOffHeapMemoryMB + "m");
			return this;
		}

		public Builder setOnHeapManagedMemory(MemorySize onHeapManagedMemory) {
			this.onHeapManagedMemory = onHeapManagedMemory;
			return this;
		}

		public Builder setOnHeapManagedMemoryMB(int onHeapManagedMemoryMB) {
			this.onHeapManagedMemory = MemorySize.parse(onHeapManagedMemoryMB + "m");
			return this;
		}

		public Builder setOffHeapManagedMemory(MemorySize offHeapManagedMemory) {
			this.offHeapManagedMemory = offHeapManagedMemory;
			return this;
		}

		public Builder setOffHeapManagedMemoryMB(int offHeapManagedMemoryMB) {
			this.offHeapManagedMemory = MemorySize.parse(offHeapManagedMemoryMB + "m");
			return this;
		}

		public Builder setGPUResource(double gpus) {
			this.gpuResource = new GPUResource(gpus);
			return this;
		}

		public ResourceSpec build() {
			return new ResourceSpec(
				cpuCores,
				taskHeapMemory,
				taskOffHeapMemory,
				onHeapManagedMemory,
				offHeapManagedMemory,
				gpuResource);
		}
	}

}
