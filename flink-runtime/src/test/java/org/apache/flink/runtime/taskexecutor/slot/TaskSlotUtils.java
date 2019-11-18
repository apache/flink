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
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Testing utility and factory methods for {@link TaskSlotTable} and {@link TaskSlot}s. */
public enum TaskSlotUtils {
	;

	private static final long DEFAULT_SLOT_TIMEOUT = 10000L;

	private static final ResourceProfile DEFAULT_RESOURCE_PROFILE =
		new ResourceProfile(
			Double.MAX_VALUE,
			MemorySize.MAX_VALUE,
			MemorySize.MAX_VALUE,
			new MemorySize(10 * MemoryManager.MIN_PAGE_SIZE),
			new MemorySize(0),
			MemorySize.MAX_VALUE,
			Collections.emptyMap());

	public static TaskSlotTable createTaskSlotTable(int numberOfSlots) {
		return createTaskSlotTable(
			numberOfSlots,
			createDefaultTimerService(DEFAULT_SLOT_TIMEOUT));
	}

	public static TaskSlotTable createTaskSlotTable(int numberOfSlots, Time timeout) {
		return createTaskSlotTable(
			numberOfSlots,
			createDefaultTimerService(timeout.toMilliseconds()));
	}

	private static TaskSlotTable createTaskSlotTable(
			int numberOfSlots,
			TimerService<AllocationID> timerService) {
		return new TaskSlotTable(createDefaultSlots(numberOfSlots), timerService);
	}

	public static TimerService<AllocationID> createDefaultTimerService(long shutdownTimeout) {
		return new TimerService<>(TestingUtils.defaultExecutor(), shutdownTimeout);
	}

	public static List<TaskSlot> createDefaultSlots(int numberOfSlots) {
		return IntStream
			.range(0, numberOfSlots)
			.mapToObj(index -> new TaskSlot(index, DEFAULT_RESOURCE_PROFILE, MemoryManager.MIN_PAGE_SIZE))
			.collect(Collectors.toList());
	}
}
