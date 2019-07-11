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

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Describe the different resource factors of the operator with UDF.
 *
 * <p>The state backend provides the method to estimate memory usages based on state size in the resource.
 *
 * <p>Resource provides {@link #merge(ResourceSpec)} method for chained operators when generating job graph.
 *
 * <p>Resource provides {@link #lessThanOrEqual(ResourceSpec)} method to compare these fields in sequence:
 * <ol>
 *     <li>CPU cores</li>
 *     <li>Heap Memory Size</li>
 *     <li>Direct Memory Size</li>
 *     <li>Native Memory Size</li>
 *     <li>State Size</li>
 *     <li>Managed Memory Size</li>
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

	/** How many java heap memory in mb are needed. */
	private final int heapMemoryInMB;

	/** How many nio direct memory in mb are needed. */
	private final int directMemoryInMB;

	/** How many native memory in mb are needed. */
	private final int nativeMemoryInMB;

	/** How many state size in mb are used. */
	private final int stateSizeInMB;

	/** The required amount of managed memory (in MB). */
	private final int managedMemoryInMB;

	private final Map<String, Resource> extendedResources = new HashMap<>(1);

	/**
	 * Creates a new ResourceSpec with full resources.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the java heap memory, in megabytes.
	 * @param directMemoryInMB The size of the java nio direct memory, in megabytes.
	 * @param nativeMemoryInMB The size of the native memory, in megabytes.
	 * @param stateSizeInMB The state size for storing in checkpoint.
	 * @param managedMemoryInMB The size of managed memory, in megabytes.
	 * @param extendedResources The extended resources, associated with the resource manager used
	 */
	private ResourceSpec(
			double cpuCores,
			int heapMemoryInMB,
			int directMemoryInMB,
			int nativeMemoryInMB,
			int stateSizeInMB,
			int managedMemoryInMB,
			Resource... extendedResources) {
		checkArgument(cpuCores >= 0, "The cpu cores of the resource spec should not be negative.");
		checkArgument(heapMemoryInMB >= 0, "The heap memory of the resource spec should not be negative");
		checkArgument(directMemoryInMB >= 0, "The direct memory of the resource spec should not be negative");
		checkArgument(nativeMemoryInMB >= 0, "The native memory of the resource spec should not be negative");
		checkArgument(stateSizeInMB >= 0, "The state size of the resource spec should not be negative");
		checkArgument(managedMemoryInMB >= 0, "The managed memory of the resource spec should not be negative");

		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = directMemoryInMB;
		this.nativeMemoryInMB = nativeMemoryInMB;
		this.stateSizeInMB = stateSizeInMB;
		this.managedMemoryInMB = managedMemoryInMB;
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
		this.heapMemoryInMB = -1;
		this.directMemoryInMB = -1;
		this.nativeMemoryInMB = -1;
		this.stateSizeInMB = -1;
		this.managedMemoryInMB = -1;
	}

	/**
	 * Used by system internally to merge the other resources of chained operators
	 * when generating the job graph or merge the resource consumed by state backend.
	 *
	 * @param other Reference to resource to merge in.
	 * @return The new resource with merged values.
	 */
	public ResourceSpec merge(ResourceSpec other) {
		if (this.equals(UNKNOWN) || other.equals(UNKNOWN)) {
			return UNKNOWN;
		}

		ResourceSpec target = new ResourceSpec(
				Math.max(this.cpuCores, other.cpuCores),
				this.heapMemoryInMB + other.heapMemoryInMB,
				this.directMemoryInMB + other.directMemoryInMB,
				this.nativeMemoryInMB + other.nativeMemoryInMB,
				this.stateSizeInMB + other.stateSizeInMB,
				this.managedMemoryInMB + other.managedMemoryInMB);
		target.extendedResources.putAll(extendedResources);
		for (Resource resource : other.extendedResources.values()) {
			target.extendedResources.merge(resource.getName(), resource, (v1, v2) -> v1.merge(v2));
		}
		return target;
	}

	public double getCpuCores() {
		return this.cpuCores;
	}

	public int getHeapMemory() {
		return this.heapMemoryInMB;
	}

	public int getDirectMemory() {
		return this.directMemoryInMB;
	}

	public int getNativeMemory() {
		return this.nativeMemoryInMB;
	}

	public int getStateSize() {
		return this.stateSizeInMB;
	}

	public int getManagedMemory() {
		return this.managedMemoryInMB;
	}

	public double getGPUResource() {
		Resource gpuResource = extendedResources.get(GPU_NAME);
		if (gpuResource != null) {
			return gpuResource.getValue();
		}

		return 0.0;
	}

	public Map<String, Resource> getExtendedResources() {
		return extendedResources;
	}

	/**
	 * Check whether all the field values are valid.
	 *
	 * @return True if all the values are equal or greater than 0, otherwise false.
	 */
	public boolean isValid() {
		if (this.cpuCores >= 0 && this.heapMemoryInMB >= 0 && this.directMemoryInMB >= 0 &&
				this.nativeMemoryInMB >= 0 && this.stateSizeInMB >= 0 && managedMemoryInMB >= 0) {
			for (Resource resource : extendedResources.values()) {
				if (resource.getValue() < 0) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks the current resource less than or equal with the other resource by comparing
	 * all the fields in the resource.
	 *
	 * @param other The resource to compare
	 * @return True if current resource is less than or equal with the other resource, otherwise return false.
	 */
	public boolean lessThanOrEqual(@Nonnull ResourceSpec other) {
		int cmp1 = Double.compare(this.cpuCores, other.cpuCores);
		int cmp2 = Integer.compare(this.heapMemoryInMB, other.heapMemoryInMB);
		int cmp3 = Integer.compare(this.directMemoryInMB, other.directMemoryInMB);
		int cmp4 = Integer.compare(this.nativeMemoryInMB, other.nativeMemoryInMB);
		int cmp5 = Integer.compare(this.stateSizeInMB, other.stateSizeInMB);
		int cmp6 = Integer.compare(this.managedMemoryInMB, other.managedMemoryInMB);
		if (cmp1 <= 0 && cmp2 <= 0 && cmp3 <= 0 && cmp4 <= 0 && cmp5 <= 0 && cmp6 <= 0) {
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
					this.heapMemoryInMB == that.heapMemoryInMB &&
					this.directMemoryInMB == that.directMemoryInMB &&
					this.nativeMemoryInMB == that.nativeMemoryInMB &&
					this.stateSizeInMB == that.stateSizeInMB &&
					this.managedMemoryInMB == that.managedMemoryInMB &&
					Objects.equals(this.extendedResources, that.extendedResources);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		final long cpuBits =  Double.doubleToLongBits(cpuCores);
		int result = (int) (cpuBits ^ (cpuBits >>> 32));
		result = 31 * result + heapMemoryInMB;
		result = 31 * result + directMemoryInMB;
		result = 31 * result + nativeMemoryInMB;
		result = 31 * result + stateSizeInMB;
		result = 31 * result + managedMemoryInMB;
		result = 31 * result + extendedResources.hashCode();
		return result;
	}

	@Override
	public String toString() {
		StringBuilder extend = new StringBuilder();
		for (Resource resource : extendedResources.values()) {
			extend.append(", ").append(resource.getName()).append("=").append(resource.getValue());
		}
		return "ResourceSpec{" +
				"cpuCores=" + cpuCores +
				", heapMemoryInMB=" + heapMemoryInMB +
				", directMemoryInMB=" + directMemoryInMB +
				", nativeMemoryInMB=" + nativeMemoryInMB +
				", stateSizeInMB=" + stateSizeInMB +
				", managedMemoryInMB=" + managedMemoryInMB + extend +
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

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for the {@link ResourceSpec}.
	 */
	public static class Builder {

		private double cpuCores;
		private int heapMemoryInMB;
		private int directMemoryInMB;
		private int nativeMemoryInMB;
		private int stateSizeInMB;
		private int managedMemoryInMB;
		private GPUResource gpuResource;

		public Builder setCpuCores(double cpuCores) {
			this.cpuCores = cpuCores;
			return this;
		}

		public Builder setHeapMemoryInMB(int heapMemory) {
			this.heapMemoryInMB = heapMemory;
			return this;
		}

		public Builder setDirectMemoryInMB(int directMemory) {
			this.directMemoryInMB = directMemory;
			return this;
		}

		public Builder setNativeMemoryInMB(int nativeMemory) {
			this.nativeMemoryInMB = nativeMemory;
			return this;
		}

		public Builder setStateSizeInMB(int stateSize) {
			this.stateSizeInMB = stateSize;
			return this;
		}

		public Builder setManagedMemoryInMB(int managedMemory) {
			this.managedMemoryInMB = managedMemory;
			return this;
		}

		public Builder setGPUResource(double gpus) {
			this.gpuResource = new GPUResource(gpus);
			return this;
		}

		public ResourceSpec build() {
			return new ResourceSpec(
				cpuCores,
				heapMemoryInMB,
				directMemoryInMB,
				nativeMemoryInMB,
				stateSizeInMB,
				managedMemoryInMB,
				gpuResource);
		}
	}

}
