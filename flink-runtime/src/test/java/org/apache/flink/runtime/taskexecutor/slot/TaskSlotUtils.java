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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.testingUtils.TestingUtils;

/** Testing utility and factory methods for {@link TaskSlotTable} and {@link TaskSlot}s. */
public enum TaskSlotUtils {
	;

	private static final long DEFAULT_SLOT_TIMEOUT = 10000L;

	public static final ResourceProfile DEFAULT_RESOURCE_PROFILE = ResourceProfile.newBuilder()
		.setCpuCores(1.0)
		.setTaskHeapMemory(new MemorySize(100 * 1024))
		.setTaskOffHeapMemory(MemorySize.ZERO)
		.setManagedMemory(new MemorySize(10 * MemoryManager.MIN_PAGE_SIZE))
		.setNetworkMemory(new MemorySize(100 * 1024))
		.build();

	public static <T extends TaskSlotPayload> TaskSlotTableImpl<T> createTaskSlotTable(int numberOfSlots) {
		return createTaskSlotTable(
			numberOfSlots,
			createDefaultTimerService());
	}

	public static <T extends TaskSlotPayload> TaskSlotTable<T> createTaskSlotTable(int numberOfSlots, Time timeout) {
		return createTaskSlotTable(
			numberOfSlots,
			createDefaultTimerService(timeout.toMilliseconds()));
	}

	private static <T extends TaskSlotPayload> TaskSlotTableImpl<T> createTaskSlotTable(
			int numberOfSlots,
			TimerService<AllocationID> timerService) {
		return new TaskSlotTableImpl<>(
			numberOfSlots,
			createTotalResourceProfile(numberOfSlots),
			DEFAULT_RESOURCE_PROFILE,
			MemoryManager.MIN_PAGE_SIZE,
			timerService,
			Executors.newDirectExecutorService());
	}

	public static ResourceProfile createTotalResourceProfile(int numberOfSlots) {
		ResourceProfile result = DEFAULT_RESOURCE_PROFILE;
		for (int i = 0; i < numberOfSlots - 1; ++i) {
			result = result.merge(DEFAULT_RESOURCE_PROFILE);
		}
		return result;
	}

	public static TimerService<AllocationID> createDefaultTimerService() {
		return createDefaultTimerService(DEFAULT_SLOT_TIMEOUT);
	}

	public static TimerService<AllocationID> createDefaultTimerService(long shutdownTimeout) {
		return new TimerService<>(TestingUtils.defaultExecutor(), shutdownTimeout);
	}
}
