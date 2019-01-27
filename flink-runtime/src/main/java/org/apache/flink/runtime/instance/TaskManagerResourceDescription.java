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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Describes the fine-grained resources of a task manager.
 */
public final class TaskManagerResourceDescription implements Serializable {

	private static final long serialVersionUID = -6827879526759191662L;

	public static final String FIELD_NAME_SIZE_CPU_CORES = "cpuCores";

	public static final String FIELD_NAME_SIZE_USER_DIRECT_MEMORY = "userDirectMemory";

	public static final String FIELD_NAME_SIZE_USER_HEAP_MEMORY = "userHeapMemory";

	public static final String FIELD_NAME_SIZE_USER_NATIVE_MEMORY = "userNativeMemory";

	public static final String FIELD_NAME_SIZE_MANAGED_MEMORY = "managedMemory";

	public static final String FIELD_NAME_SIZE_NETWORK_MEMORY = "networkMemory";

	public double getNumberOfCPUCores() {
		return numberOfCPUCores;
	}

	public long getSizeOfUserDirectMemory() {
		return sizeOfUserDirectMemory;
	}

	public long getSizeOfUserHeapMemory() {
		return sizeOfUserHeapMemory;
	}

	public long getSizeOfUserNativeMemory() {
		return sizeOfUserNativeMemory;
	}

	public long getSizeOfManagedMemory() {
		return sizeOfManagedMemory;
	}

	public long getSizeOfNetworkMemory() {
		return sizeOfNetworkMemory;
	}

	@JsonProperty(FIELD_NAME_SIZE_CPU_CORES)
	private final double numberOfCPUCores;

	@JsonProperty(FIELD_NAME_SIZE_USER_DIRECT_MEMORY)
	private final long sizeOfUserDirectMemory;

	@JsonProperty(FIELD_NAME_SIZE_USER_HEAP_MEMORY)
	private final long sizeOfUserHeapMemory;

	@JsonProperty(FIELD_NAME_SIZE_USER_NATIVE_MEMORY)
	private final long sizeOfUserNativeMemory;

	@JsonProperty(FIELD_NAME_SIZE_MANAGED_MEMORY)
	private final long sizeOfManagedMemory;

	@JsonProperty(FIELD_NAME_SIZE_NETWORK_MEMORY)
	private final long sizeOfNetworkMemory;

	/**
	 * Constructs a new task manager resource description object.
	 */
	@JsonCreator
	public TaskManagerResourceDescription(
			@JsonProperty(FIELD_NAME_SIZE_CPU_CORES) double numberOfCPUCores,
			@JsonProperty(FIELD_NAME_SIZE_USER_DIRECT_MEMORY) long sizeOfUserDirectMemory,
			@JsonProperty(FIELD_NAME_SIZE_USER_HEAP_MEMORY) long sizeOfUserHeapMemory,
			@JsonProperty(FIELD_NAME_SIZE_USER_NATIVE_MEMORY) long sizeOfUserNativeMemory,
			@JsonProperty(FIELD_NAME_SIZE_MANAGED_MEMORY) long sizeOfManagedMemory,
			@JsonProperty(FIELD_NAME_SIZE_NETWORK_MEMORY) long sizeOfNetworkMemory) {
		this.numberOfCPUCores = numberOfCPUCores;
		this.sizeOfUserDirectMemory = sizeOfUserDirectMemory;
		this.sizeOfUserHeapMemory = sizeOfUserHeapMemory;
		this.sizeOfUserNativeMemory = sizeOfUserNativeMemory;
		this.sizeOfManagedMemory = sizeOfManagedMemory;
		this.sizeOfNetworkMemory = sizeOfNetworkMemory;
	}

	public static TaskManagerResourceDescription fromResourceProfile(ResourceProfile resourceProfile) {
		return new TaskManagerResourceDescription(resourceProfile.getCpuCores(),
			((long) resourceProfile.getDirectMemoryInMB()) << 20,
			((long) resourceProfile.getHeapMemoryInMB()) << 20,
			((long) resourceProfile.getNativeMemoryInMB()) << 20,
			((long) resourceProfile.getManagedMemoryInMB()) << 20,
			((long) resourceProfile.getNetworkMemoryInMB()) << 20);
	}

	// --------------------------------------------------------------------------------------------
	// Utils
	// --------------------------------------------------------------------------------------------


	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TaskManagerResourceDescription that = (TaskManagerResourceDescription) o;
		return numberOfCPUCores == that.numberOfCPUCores &&
			sizeOfUserDirectMemory == that.sizeOfUserDirectMemory &&
			sizeOfUserHeapMemory == that.sizeOfUserHeapMemory &&
			sizeOfUserNativeMemory == that.sizeOfUserNativeMemory &&
			sizeOfManagedMemory == that.sizeOfManagedMemory &&
			sizeOfNetworkMemory == that.sizeOfNetworkMemory;
	}

	@Override
	public int hashCode() {
		return Objects.hash(numberOfCPUCores, sizeOfUserDirectMemory, sizeOfUserHeapMemory, sizeOfUserNativeMemory,
			sizeOfManagedMemory, sizeOfNetworkMemory);
	}

	@Override
	public String toString() {
		return String.format("cores=%s, userDirect=%d, userHeap=%d, userNative=%d, managed=%d, network=%d",
			numberOfCPUCores, sizeOfUserDirectMemory, sizeOfUserHeapMemory, sizeOfUserNativeMemory,
			sizeOfManagedMemory, sizeOfNetworkMemory);
	}
}
