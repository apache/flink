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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.standalone.TaskManagerResourceCalculator;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Describe the resource of a task manager, including the task resource and framework resource.
 * The task resource is represented by the ResourceProfile, and the framework resource including:
 * private memory for the task manager process and netty framework.
 */
public class TaskManagerResource {

	/** The netty memory for TaskManager process. */
	private final int taskManagerNettyMemorySizeMB;

	/** The reserved native memory for TaskManager process. */
	private final int taskManagerNativeMemorySizeMB;

	/** The reserved heap memory for TaskManager process. */
	private final int taskManagerHeapMemorySizeMB;

	/** Fraction of memory allocated by the memory manager. */
	private final float managedMemoryFraction;

	/** The memory type used for NetworkBufferPool and MemoryManager, heap or off-heap. */
	private final boolean offHeap;

	/** Whether TaskManager managed memory should be pre-allocated when the TaskManager is starting. */
	private final boolean managedMemoryPreAllocate;

	/** The ratio for young generation of persistent memory. */
	private final double persistentYoungHeapRatio;

	/** The ratio for young generation of dynamic memory. */
	private final double dynamicYoungHeapRatio;

	/** The resource profile of running tasks in TaskManager. */
	private ResourceProfile taskResourceProfile;

	/** The number of slots in TaskManager. */
	private final int slotNum;

	/** The default network memory size in MB when no network memory is specified in resource profile. */
	private final int defaultNetworkMemMB;

	/** The default heap memory size in MB when no heap memory is specified in resource profile. */
	private final int defaultHeapMemMB;

	/** The user specified total TaskManager memory size.
	 *  In session mode, it could be specified by tm argument.
	 *  In perjob mode, it will be dynamic calculated based on resource profile and framework overhead.
	 * */
	private int taskManagerTotalMemoryMB;

	private TaskManagerResource(
		int taskManagerNettyMemorySizeMB,
		int taskManagerNativeMemorySizeMB,
		int taskManagerHeapMemorySizeMB,
		float managedMemoryFraction,
		boolean offHeap,
		boolean preAllocate,
		int defaultNetworkMemMB,
		int defaultHeapMemMB,
		double dynamicYoungHeapRatio,
		double persistentYoungHeapRatio,
		ResourceProfile taskResourceProfile,
		int slotNum,
		int taskManagerTotalMemoryMB) {

		this.taskManagerNettyMemorySizeMB = taskManagerNettyMemorySizeMB;
		this.taskManagerNativeMemorySizeMB = taskManagerNativeMemorySizeMB;
		this.taskManagerHeapMemorySizeMB = taskManagerHeapMemorySizeMB;
		this.managedMemoryFraction = managedMemoryFraction;
		this.offHeap = offHeap;
		this.managedMemoryPreAllocate = preAllocate;
		this.defaultNetworkMemMB = defaultNetworkMemMB;
		this.defaultHeapMemMB = defaultHeapMemMB;
		this.taskResourceProfile = taskResourceProfile;
		this.slotNum = slotNum;
		this.persistentYoungHeapRatio = persistentYoungHeapRatio;
		this.dynamicYoungHeapRatio = dynamicYoungHeapRatio;
		this.taskManagerTotalMemoryMB = taskManagerTotalMemoryMB;
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public int getTaskManagerNettyMemorySizeMB() {
		return taskManagerNettyMemorySizeMB;
	}

	public int getTotalContainerMemory() {
		if (taskManagerTotalMemoryMB > 0) {
			return taskManagerTotalMemoryMB;
		}
		return getTotalHeapMemory() + getTotalDirectMemory() + getTotalNativeMemory();
	}

	/**
	 * @return managedMemory size
	 */
	public int getManagedMemorySize() {
		return taskResourceProfile.getManagedMemoryInMB() * slotNum;
	}

	public int getFloatingManagedMemorySize() {
		return taskResourceProfile.getFloatingManagedMemoryInMB() * slotNum;
	}

	/**
	 * Get the memory for network buffer pool, it is calculated by job master according to the input and output channel number.
	 * @return the network memory for all tasks in the task executor.
	 */
	public int getNetworkMemorySize() {
		int networkMemoryInMB = taskResourceProfile.getNetworkMemoryInMB() * slotNum;
		return networkMemoryInMB > 0 ? networkMemoryInMB : defaultNetworkMemMB;
	}

	public int getTotalNativeMemory() {
		return taskResourceProfile.getNativeMemoryInMB() * slotNum + taskManagerNativeMemorySizeMB;
	}

	/**
	 * We separate heap memory into to two parts: dynamic memory and persistent memory.
	 * persistent memory is memory used by buffer pool, which will never be freed once allocated,
	 * and will always in old generation, the memory left is dynamic memory, which will be used for
	 * creating objects which will be created and released frequently, and exists in both old and
	 * new generation.
	 * -----------------------------------------------
	 *             Heap Memory
	 * -----------------------------------------------
	 *      Young          |          OLD
	 * -----------------------------------------------
	 *      extra   |   dynamic    |    persistent
	 * -----------------------------------------------
	 * dynamic memory in young is controlled by dynamicYoungHeapRatio. we add some extra memory to young
	 * to make sure that young generation is larger than persistent * persistentYoungHeapRatio, which make
	 * TaskManager more GC friendly when persistent memory is not pre-allocated.
	 */
	public int getTotalHeapMemory() {
		if (taskManagerTotalMemoryMB > 0) {
			int heap = taskManagerTotalMemoryMB - getTotalNativeMemory() - getTotalDirectMemory();
			Preconditions.checkArgument(heap > 0,
				String.format("Total task manager memory(%s) should be more than the sum of native(%s) and direct(%s)",
				taskManagerTotalMemoryMB, getTotalNativeMemory(), getTotalDirectMemory()));
			return heap;
		}
		int heapMemory =  taskManagerHeapMemorySizeMB + (taskResourceProfile.getHeapMemoryInMB() > 0 ?
			taskResourceProfile.getHeapMemoryInMB() * slotNum : defaultHeapMemMB);
		if (!offHeap) {
			heapMemory += getManagedMemorySize() + getFloatingManagedMemorySize();
		}
		return heapMemory;
	}

	public int getTotalDirectMemory() {
		int directMemory = taskManagerNettyMemorySizeMB + taskResourceProfile.getDirectMemoryInMB() * slotNum + getNetworkMemorySize();
		if (offHeap) {
			directMemory += getManagedMemorySize() + getFloatingManagedMemorySize();
		}
		return directMemory;
	}

	public int getYoungHeapMemory() {
		int dynamicMemory = getDynamicHeapMemory();
		int persistentMemory = getPersistentHeapMemory();

		// we will ensure the young generation size large than given fraction of persistentMemory in case of we may
		// allocate persistent memory dynamically, which will cause too much young gc.
		return (int) Math.max(dynamicMemory * dynamicYoungHeapRatio, persistentMemory * persistentYoungHeapRatio);
	}

	/**
	 * Persistent Memory means memory in heap which will never be released once allocated, we need to prepare old
	 * generation memory for them only.
	 *
	 * @return size of the persistent memory
	 */
	private int getPersistentHeapMemory() {
		int persistentMemory = 0;
		if (!offHeap && managedMemoryPreAllocate) {
			persistentMemory += getManagedMemorySize() + getFloatingManagedMemorySize();
		}
		return persistentMemory;
	}

	/**
	 * Dynamic Memory means memory in heap which will be manipulate frequently, and we need to prepare both young
	 * and old generation memory for them.
	 *
	 * @return size of the dynamic memory
	 */
	private int getDynamicHeapMemory() {
		return getTotalHeapMemory() - getPersistentHeapMemory();
	}

	public int getSlotNum() {
		return slotNum;
	}

	public ResourceProfile getTaskResourceProfile() {
		int userHeap = taskResourceProfile.getHeapMemoryInMB();
		if (taskManagerTotalMemoryMB > 0) {
			if (offHeap) {
				userHeap = getTotalHeapMemory() - taskManagerHeapMemorySizeMB;
			} else {
				userHeap = getTotalHeapMemory() - taskManagerHeapMemorySizeMB - getManagedMemorySize() - getFloatingManagedMemorySize();
			}
		}
		return new ResourceProfile(taskResourceProfile.getCpuCores(), userHeap,
			taskResourceProfile.getDirectMemoryInMB(), taskResourceProfile.getNativeMemoryInMB(),
			taskResourceProfile.getNetworkMemoryInMB(), taskResourceProfile.getExtendedResources());
	}

	public double getContainerCpuCores() {
		return taskResourceProfile.getCpuCores() * slotNum;
	}

	// --------------------------------------------------------------------------------------------
	//  Parsing configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method to parse the configuration for task manager resource.
	 *
	 * @param configuration The configuration.
	 * @param resourceProfile The resource profile of task in the task manager.
	 * @param slotNum Slot number in each task manager.
	 * @param taskManagerTotalMemorySize Total memory size of task manager, user specified in session and -1 in perJob.
	 * @return TaskManagerResourceProfile
	 */
	public static TaskManagerResource fromConfiguration(
			Configuration configuration,
			ResourceProfile resourceProfile,
			int slotNum,
			int taskManagerTotalMemorySize) {

		final int taskManagerNettyMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY);
		final int taskManagerNativeMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY);
		final int taskManagerHeapMemorySizeMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY);

		double dynamicYoungRatio = configuration.getDouble(
				TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO);
		double persistentYoungRatio = configuration.getDouble(
				TaskManagerOptions.TASK_MANAGER_MEMORY_PERSISTENT_YOUNG_RATIO);

		float managedMemoryFraction = configuration.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);

		// check whether we use heap or off-heap memory
		final boolean useOffHeap = configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);

		final boolean preAllocate = configuration.getBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE);

		final int defaultNetworkMemMB = (int) Math.ceil(TaskManagerResourceCalculator.calculateNetworkBufferMemory(configuration) / (1024.0 * 1024.0));

		final int defaultHeapMemMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);

		return new TaskManagerResource(
				taskManagerNettyMemorySizeMB,
				taskManagerNativeMemorySizeMB,
				taskManagerHeapMemorySizeMB,
				managedMemoryFraction,
				useOffHeap,
			  preAllocate,
				defaultNetworkMemMB,
				defaultHeapMemMB,
				dynamicYoungRatio,
				persistentYoungRatio,
				resourceProfile,
				slotNum,
			  taskManagerTotalMemorySize);
	}

	public static TaskManagerResource fromConfiguration(
		Configuration configuration,
		ResourceProfile resourceProfile,
		int slotNum) {
		return fromConfiguration(configuration, resourceProfile, slotNum, -1);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				taskManagerNettyMemorySizeMB, offHeap, taskManagerNativeMemorySizeMB, taskManagerHeapMemorySizeMB,
				dynamicYoungHeapRatio, persistentYoungHeapRatio, taskResourceProfile, slotNum);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == TaskManagerResource.class) {
			TaskManagerResource that = (TaskManagerResource) obj;
			return this.taskResourceProfile.equals(that.taskResourceProfile) &&
					this.taskManagerNettyMemorySizeMB == that.taskManagerNettyMemorySizeMB &&
					this.taskManagerNativeMemorySizeMB == that.taskManagerNativeMemorySizeMB &&
					this.taskManagerHeapMemorySizeMB == that.taskManagerHeapMemorySizeMB &&
					this.offHeap == that.offHeap &&
					this.managedMemoryFraction == that.managedMemoryFraction &&
					this.slotNum == that.slotNum &&
					this.dynamicYoungHeapRatio == that.dynamicYoungHeapRatio &&
					this.persistentYoungHeapRatio == that.persistentYoungHeapRatio;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "TaskManagerResource {" +
			  "taskManagerTotalMemoryMB=" + taskManagerTotalMemoryMB +
				", taskResourceProfile=" + getTaskResourceProfile() +
				", taskManagerNettyMemoryInMB=" + taskManagerNettyMemorySizeMB +
				", taskManagerNativeMemorySizeMB=" + taskManagerNativeMemorySizeMB +
				", taskManagerHeapMemorySizeMB=" + taskManagerHeapMemorySizeMB +
				", dynamicYoungRatio=" + dynamicYoungHeapRatio +
				", persistentYoungRatio=" + persistentYoungHeapRatio +
				", offHeap=" + offHeap +
				", slotNum=" + slotNum +
				'}';
	}

}
