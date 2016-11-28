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
 * Describe the different resource factors of the operator.
 * The resource may be merged for chained operators when generating job graph.
 *
 * <p>Resource provides {@link #lessThan(ResourceSpec)} method to compare these fields in sequence:
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

	public static final ResourceSpec UNKNOWN = new ResourceSpec(-1.0, -1L, -1L, -1L, -1L);

	/** How many cpu cores are needed, use double so we can specify cpu like 0.1 */
	private double cpuCores;

	/** How many java heap memory in mb are needed */
	private long heapMemoryInMB;

	/** How many nio direct memory in mb are needed */
	private long directMemoryInMB;

	/** How many native memory in mb are needed */
	private long nativeMemoryInMB;

	/** How many state size are used */
	private long stateSize;

	/**
	 * Creates a new ResourceSpec with basic common resources.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the java heap memory, in megabytes.
	 */
	public ResourceSpec(double cpuCores, long heapMemoryInMB) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
	}

	/**
	 * Creates a new ResourceSpec with full resources.
	 *
	 * @param cpuCores The number of CPU cores (possibly fractional, i.e., 0.2 cores)
	 * @param heapMemoryInMB The size of the java heap memory, in megabytes.
	 * @param directMemoryInMB The size of the java nio direct memory, in megabytes.
	 * @param nativeMemoryInMB The size of the native memory, in megabytes.
	 * @param stateSize The state size for storing in checkpoint.
	 */
	public ResourceSpec(
			double cpuCores,
			long heapMemoryInMB,
			long directMemoryInMB,
			long nativeMemoryInMB,
			long stateSize) {
		this.cpuCores = cpuCores;
		this.heapMemoryInMB = heapMemoryInMB;
		this.directMemoryInMB = directMemoryInMB;
		this.nativeMemoryInMB = nativeMemoryInMB;
		this.stateSize = stateSize;
	}

	public void merge(ResourceSpec other) {
		this.cpuCores = Math.max(this.cpuCores, other.getCpuCores());
		this.heapMemoryInMB += other.getHeapMemory();
		this.directMemoryInMB += other.getDirectMemory();
		this.nativeMemoryInMB += other.getNativeMemory();
		this.stateSize += other.getStateSize();
	}

	public double getCpuCores() {
		return this.cpuCores;
	}

	public long getHeapMemory() {
		return this.heapMemoryInMB;
	}

	public long getDirectMemory() {
		return this.directMemoryInMB;
	}

	public long getNativeMemory() {
		return this.nativeMemoryInMB;
	}

	public long getStateSize() {
		return this.stateSize;
	}

	public boolean lessThan(@Nonnull ResourceSpec other) {
		int cmp1 = Double.compare(this.cpuCores, other.cpuCores);
		int cmp2 = Long.compare(this.heapMemoryInMB, other.heapMemoryInMB);
		int cmp3 = Long.compare(this.directMemoryInMB, other.directMemoryInMB);
		int cmp4 = Long.compare(this.nativeMemoryInMB, other.nativeMemoryInMB);
		int cmp5 = Long.compare(this.stateSize, other.stateSize);
		if (cmp1 <= 0 && cmp2 <= 0 && cmp3 <= 0 && cmp4 <= 0 && cmp5 <= 0) {
			return true;
		} else {
			return false;
		}
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
				this.stateSize == that.stateSize;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "ResourceSpec{" +
			"cpuCores=" + cpuCores +
			", heapMemoryInMB=" + heapMemoryInMB +
			", directMemoryInMB=" + directMemoryInMB +
			", nativeMemoryInMB=" + nativeMemoryInMB +
			", stateSize=" + stateSize +
			'}';
	}
}
