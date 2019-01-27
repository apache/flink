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

package org.apache.flink.table.plan.nodes.exec;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;

/**
 * Resource for node: parallelism, cpu, heap and direct memory, reserved managed memory, prefer managed memory and
 * max managed memory.
 * Reserved managed memory: needed when an operator init.
 * Prefer managed memory: tell the scheduler how much managed memory an operator may use.
 * Max managed memory: max managed memory that an operator can use.
 * Node use it to build {@link ResourceSpec} to set {@link org.apache.flink.streaming.api.transformations.StreamTransformation}.
 */
public class NodeResource {

	// node parallelism
	private int parallelism = -1;

	// node cpu
	private double cpu;

	// node heap memory
	private int heapMem;

	// node direct memory
	private int directMem;

	// reserved managed mem for node
	private int reservedManagedMem;

	// prefer managed mem for node
	private int preferManagedMem;

	// max managed mem for node
	private int maxManagedMem;

	public void setCpu(double cpu) {
		this.cpu = cpu;
	}

	public double getCpu() {
		return cpu;
	}

	public void setHeapMem(int heapMem) {
		this.heapMem = heapMem;
	}

	public int getReservedManagedMem() {
		return reservedManagedMem;
	}

	public void setManagedMem(int reservedManagedMem, int preferManagedMem, int maxManagedMem) {
		this.reservedManagedMem = reservedManagedMem;
		this.preferManagedMem = preferManagedMem;
		this.maxManagedMem = maxManagedMem;
	}

	public int getPreferManagedMem() {
		return preferManagedMem;
	}

	public int getParallelism() {
		return parallelism;
	}

	public int getMaxManagedMem() {
		return maxManagedMem;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public void setDirectMem(int directMem) {
		this.directMem = directMem;
	}

	public ResourceSpec getReservedResourceSpec() {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heapMem);
		builder.setDirectMemoryInMB(directMem);
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
				getPreferManagedMem() - getReservedManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		return builder.build();
	}

	public ResourceSpec getPreferResourceSpec() {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setCpuCores(cpu);
		builder.setHeapMemoryInMB(heapMem);
		builder.setDirectMemoryInMB(directMem);
		builder.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME,
				getPreferManagedMem(),
				Resource.ResourceAggregateType.AGGREGATE_TYPE_SUM));
		return builder.build();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("NodeResource{");
		if (parallelism > 0) {
			sb.append("parallelism=").append(parallelism);
		}
		if (cpu > 0) {
			sb.append(", cpu=").append(cpu);
		}
		if (heapMem > 0) {
			sb.append(", heapMem=").append(heapMem);
		}
		if (directMem > 0) {
			sb.append(", directMem=").append(directMem);
		}
		if (reservedManagedMem > 0) {
			sb.append(", reservedManagedMem=").append(reservedManagedMem);
		}
		if (preferManagedMem > 0) {
			sb.append(", preferManagedMem=").append(preferManagedMem);
		}
		if (maxManagedMem > 0) {
			sb.append(", maxManagedMem=").append(maxManagedMem);
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		NodeResource resource = (NodeResource) o;

		if (parallelism != resource.parallelism) {
			return false;
		}
		if (Double.compare(resource.cpu, cpu) != 0) {
			return false;
		}
		if (heapMem != resource.heapMem) {
			return false;
		}
		if (directMem != resource.directMem) {
			return false;
		}
		if (reservedManagedMem != resource.reservedManagedMem) {
			return false;
		}
		if (preferManagedMem != resource.preferManagedMem) {
			return false;
		}
		return maxManagedMem == resource.maxManagedMem;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = parallelism;
		temp = Double.doubleToLongBits(cpu);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + heapMem;
		result = 31 * result + directMem;
		result = 31 * result + reservedManagedMem;
		result = 31 * result + preferManagedMem;
		result = 31 * result + maxManagedMem;
		return result;
	}
}
