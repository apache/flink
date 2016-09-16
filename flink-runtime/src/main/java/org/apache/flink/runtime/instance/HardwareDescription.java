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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.util.Hardware;

import java.io.Serializable;

/**
 * A hardware description describes the resources available to a task manager.
 */
public final class HardwareDescription implements Serializable {

	private static final long serialVersionUID = 3380016608300325361L;

	/** The number of CPU cores available to the JVM on the compute node. */
	private final int numberOfCPUCores;

	/** The size of physical memory in bytes available on the compute node. */
	private final long sizeOfPhysicalMemory;

	/** The size of the JVM heap memory */
	private final long sizeOfJvmHeap;

	/** The size of the memory managed by the system for caching, hashing, sorting, ... */
	private final long sizeOfManagedMemory;

	/**
	 * Constructs a new hardware description object.
	 * 
	 * @param numberOfCPUCores The number of CPU cores available to the JVM on the compute node. 
	 * @param sizeOfPhysicalMemory The size of physical memory in bytes available on the compute node.
	 * @param sizeOfJvmHeap The size of the JVM heap memory.
	 * @param sizeOfManagedMemory The size of the memory managed by the system for caching, hashing, sorting, ...
	 */
	public HardwareDescription(int numberOfCPUCores, long sizeOfPhysicalMemory, long sizeOfJvmHeap, long sizeOfManagedMemory) {
		this.numberOfCPUCores = numberOfCPUCores;
		this.sizeOfPhysicalMemory = sizeOfPhysicalMemory;
		this.sizeOfJvmHeap = sizeOfJvmHeap;
		this.sizeOfManagedMemory = sizeOfManagedMemory;
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
	 * Returns the size of the JVM heap memory
	 * 
	 * @return The size of the JVM heap memory
	 */
	public long getSizeOfJvmHeap() {
		return this.sizeOfJvmHeap;
	}

	/**
	 * Returns the size of the memory managed by the system for caching, hashing, sorting, ...
	 * 
	 * @return The size of the memory managed by the system.
	 */
	public long getSizeOfManagedMemory() {
		return this.sizeOfManagedMemory;
	}

	// --------------------------------------------------------------------------------------------
	// Utils
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("cores=%d, physMem=%d, heap=%d, managed=%d", 
				numberOfCPUCores, sizeOfPhysicalMemory, sizeOfJvmHeap, sizeOfManagedMemory);
	}

	// --------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------

	public static HardwareDescription extractFromSystem(long managedMemory) {
		final int numberOfCPUCores = Hardware.getNumberCPUCores();
		final long sizeOfJvmHeap = Runtime.getRuntime().maxMemory();
		final long sizeOfPhysicalMemory = Hardware.getSizeOfPhysicalMemory();

		return new HardwareDescription(numberOfCPUCores, sizeOfPhysicalMemory, sizeOfJvmHeap, managedMemory);
	}
}
