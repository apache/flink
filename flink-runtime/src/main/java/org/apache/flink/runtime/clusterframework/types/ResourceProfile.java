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

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * Describe the resource profile of the slot, either when requiring or offering it. The profile can be
 * checked whether it can match another profile's requirement, and furthermore we may calculate a matching
 * score to decide which profile we should choose when we have lots of candidate slots.
 * 
 * <p>Resource Profiles have a total ordering, defined by comparing these fields in sequence:
 * <ol>
 *     <li>Memory Size</li>
 *     <li>CPU cores</li>
 * </ol>
 */
public class ResourceProfile implements Serializable, Comparable<ResourceProfile> {

	private static final long serialVersionUID = 1L;

	public static final ResourceProfile UNKNOWN = new ResourceProfile(-1.0, -1, -1, -1);

	// ------------------------------------------------------------------------

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1 */
	private final double cpuCores;

	/** How many heap memory in mb are needed */
	private final int heapMemoryInMB;

	/** How many direct memory in mb are needed */
	private final int directMemoryInMB;

	/** How many native memory in mb are needed */
	private final int nativeMemoryInMB;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new ResourceProfile.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the heap memory, in megabytes.
	 * @param directMemoryInMB The size of the direct memory, in megabytes.
	 * @param nativeMemoryInMB The size of the native memory, in megabytes.
	 */
	public ResourceProfile(double cpuCores, int heapMemoryInMB, int directMemoryInMB, int nativeMemoryInMB) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = directMemoryInMB;
		this.nativeMemoryInMB = nativeMemoryInMB;
	}

	/**
	 * Creates a new ResourceProfile.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the heap memory, in megabytes.
	 */
	public ResourceProfile(double cpuCores, int heapMemoryInMB) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = 0;
		this.nativeMemoryInMB = 0;
	}

	/**
	 * Creates a copy of the given ResourceProfile.
	 * 
	 * @param other The ResourceProfile to copy. 
	 */
	public ResourceProfile(ResourceProfile other) {
		this.cpuCores = other.cpuCores;
		this.heapMemoryInMB = other.heapMemoryInMB;
		this.directMemoryInMB = other.directMemoryInMB;
		this.nativeMemoryInMB = other.nativeMemoryInMB;
	}

	// ------------------------------------------------------------------------

	/**
	 * Get the cpu cores needed
	 * @return The cpu cores, 1.0 means a full cpu thread
	 */
	public double getCpuCores() {
		return cpuCores;
	}

	/**
	 * Get the heap memory needed in MB
	 * @return The heap memory in MB
	 */
	public int getHeapMemoryInMB() {
		return heapMemoryInMB;
	}

	/**
	 * Get the direct memory needed in MB
	 * @return The direct memory in MB
	 */
	public int getDirectMemoryInMB() {
		return directMemoryInMB;
	}

	/**
	 * Get the native memory needed in MB
	 * @return The native memory in MB
	 */
	public int getNativeMemoryInMB() {
		return nativeMemoryInMB;
	}

	/**
	 * Check whether required resource profile can be matched
	 *
	 * @param required the required resource profile
	 * @return true if the requirement is matched, otherwise false
	 */
	public boolean isMatching(ResourceProfile required) {
		return cpuCores >= required.getCpuCores() &&
			heapMemoryInMB >= required.getHeapMemoryInMB() &&
			directMemoryInMB >= required.getDirectMemoryInMB() &&
			nativeMemoryInMB >= required.getNativeMemoryInMB();
	}

	@Override
	public int compareTo(@Nonnull ResourceProfile other) {
		int cmp1 = Integer.compare(this.heapMemoryInMB + this.directMemoryInMB + this.nativeMemoryInMB,
			other.heapMemoryInMB + other.directMemoryInMB + other.nativeMemoryInMB);
		int cmp2 = Double.compare(this.cpuCores, other.cpuCores);
		return (cmp1 != 0) ? cmp1 : cmp2; 
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return Objects.hash(cpuCores, heapMemoryInMB, directMemoryInMB, nativeMemoryInMB);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == ResourceProfile.class) {
			ResourceProfile that = (ResourceProfile) obj;
			return this.cpuCores == that.cpuCores &&
				this.heapMemoryInMB == that.heapMemoryInMB &&
				this.directMemoryInMB == that.directMemoryInMB &&
				this.nativeMemoryInMB == that.nativeMemoryInMB;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "ResourceProfile{" +
			"cpuCores=" + cpuCores +
			", heapMemoryInMB=" + heapMemoryInMB +
			", directMemoryInMB=" + directMemoryInMB +
			", nativeMemoryInMB=" + nativeMemoryInMB +
			'}';
	}
}
