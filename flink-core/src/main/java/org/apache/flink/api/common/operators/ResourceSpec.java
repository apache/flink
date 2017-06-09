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

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Describe the different resource factors of the operator with UDF.
 *
 * The state backend provides the method to estimate memory usages based on state size in the resource.
 *
 * Resource provides {@link #merge(ResourceSpec)} method for chained operators when generating job graph.
 *
 * <p>Resource provides {@link #lessThanOrEqual(ResourceSpec)} method to compare these fields in sequence:
 * <ol>
 *     <li>CPU cores</li>
 *     <li>Heap Memory Size</li>
 *     <li>Direct Memory Size</li>
 *     <li>Native Memory Size</li>
 *     <li>State Size</li>
 * </ol>
 */
@Internal
public class ResourceSpec implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final ResourceSpec DEFAULT = new ResourceSpec(0, 0, 0, 0, 0);

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1 */
	private final double cpuCores;

	/** How many java heap memory in mb are needed */
	private final int heapMemoryInMB;

	/** How many nio direct memory in mb are needed */
	private final int directMemoryInMB;

	/** How many native memory in mb are needed */
	private final int nativeMemoryInMB;

	/** How many state size in mb are used */
	private final int stateSizeInMB;

	/**
	 * Creates a new ResourceSpec with basic common resources.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the java heap memory, in megabytes.
	 */
	public ResourceSpec(double cpuCores, int heapMemoryInMB) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = 0;
		this.nativeMemoryInMB = 0;
		this.stateSizeInMB = 0;
	}

	/**
	 * Creates a new ResourceSpec with full resources.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the java heap memory, in megabytes.
	 * @param directMemoryInMB The size of the java nio direct memory, in megabytes.
	 * @param nativeMemoryInMB The size of the native memory, in megabytes.
	 * @param stateSizeInMB The state size for storing in checkpoint.
	 */
	public ResourceSpec(
			double cpuCores,
			int heapMemoryInMB,
			int directMemoryInMB,
			int nativeMemoryInMB,
			int stateSizeInMB) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = directMemoryInMB;
		this.nativeMemoryInMB = nativeMemoryInMB;
		this.stateSizeInMB = stateSizeInMB;
	}

	/**
	 * Used by system internally to merge the other resources of chained operators
	 * when generating the job graph or merge the resource consumed by state backend.
	 *
	 * @param other Reference to resource to merge in.
	 * @return The new resource with merged values.
	 */
	public ResourceSpec merge(ResourceSpec other) {
		return new ResourceSpec(
				Math.max(this.cpuCores, other.cpuCores),
				this.heapMemoryInMB + other.heapMemoryInMB,
				this.directMemoryInMB + other.directMemoryInMB,
				this.nativeMemoryInMB + other.nativeMemoryInMB,
				this.stateSizeInMB + other.stateSizeInMB);
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

	/**
	 * Check whether all the field values are valid.
	 *
	 * @return True if all the values are equal or greater than 0, otherwise false.
	 */
	public boolean isValid() {
		return (this.cpuCores >= 0 && this.heapMemoryInMB >= 0 && this.directMemoryInMB >= 0 &&
				this.nativeMemoryInMB >= 0 && this.stateSizeInMB >= 0);
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
		return (cmp1 <= 0 && cmp2 <= 0 && cmp3 <= 0 && cmp4 <= 0 && cmp5 <= 0);
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
					this.stateSizeInMB == that.stateSizeInMB;
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
		return result;
	}

	@Override
	public String toString() {
		return "ResourceSpec{" +
				"cpuCores=" + cpuCores +
				", heapMemoryInMB=" + heapMemoryInMB +
				", directMemoryInMB=" + directMemoryInMB +
				", nativeMemoryInMB=" + nativeMemoryInMB +
				", stateSizeInMB=" + stateSizeInMB +
				'}';
	}
}
