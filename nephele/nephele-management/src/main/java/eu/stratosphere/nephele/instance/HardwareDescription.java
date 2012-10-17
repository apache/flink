/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;

/**
 * A hardware description reflects the hardware environment which is actually present on the task manager's compute
 * nodes. Unlike the {@link InstanceType} the hardware description is determined by the compute node itself and not
 * loaded from a predefined configuration profile. In particular, the hardware description includes the size of free
 * memory which is actually available to the JVM and can be used to allocate large memory portions.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class HardwareDescription {

	/**
	 * The number of CPU cores available to the JVM on the compute node.
	 */
	private final int numberOfCPUCores;

	/**
	 * The size of physical memory in bytes available on the compute node.
	 */
	private long sizeOfPhysicalMemory;

	/**
	 * The size of free memory in bytes available to the JVM on the compute node.
	 */
	private long sizeOfFreeMemory;

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private HardwareDescription() {
		this.numberOfCPUCores = 0;
		this.sizeOfPhysicalMemory = 0L;
		this.sizeOfFreeMemory = 0L;
	}

	/**
	 * Constructs a new hardware description object.
	 * 
	 * @param numberOfCPUCores
	 *        the number of CPU cores available to the JVM on the compute node
	 * @param sizeOfPhysicalMemory
	 *        the size of physical memory in bytes available on the compute node
	 * @param sizeOfFreeMemory
	 *        the size of free memory in bytes available to the JVM on the compute node
	 */
	HardwareDescription(final int numberOfCPUCores, final long sizeOfPhysicalMemory, final long sizeOfFreeMemory) {
		this.numberOfCPUCores = numberOfCPUCores;
		this.sizeOfPhysicalMemory = sizeOfPhysicalMemory;
		this.sizeOfFreeMemory = sizeOfFreeMemory;
	}

	/**
	 * Returns the number of CPU cores available to the JVM on the compute node.
	 * 
	 * @return the number of CPU cores available to the JVM on the compute node
	 */
	public int getNumberOfCPUCores() {
		return this.numberOfCPUCores;
	}

	/**
	 * Returns the size of physical memory in bytes available on the compute node.
	 * 
	 * @return the size of physical memory in bytes available on the compute node
	 */
	public long getSizeOfPhysicalMemory() {
		return this.sizeOfPhysicalMemory;
	}

	/**
	 * Returns the size of free memory in bytes available to the JVM on the compute node.
	 * 
	 * @return the size of free memory in bytes available to the JVM on the compute node
	 */
	public long getSizeOfFreeMemory() {
		return this.sizeOfFreeMemory;
	}
}
