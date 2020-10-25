/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Container for {@link SlotInfo} and the task executors utilization (freeSlots / totalOfferedSlots).
 */
public final class SlotInfoWithUtilization implements SlotInfo {
	private final SlotInfo slotInfoDelegate;
	private final double taskExecutorUtilization;

	private SlotInfoWithUtilization(SlotInfo slotInfo, double taskExecutorUtilization) {
		this.slotInfoDelegate = slotInfo;
		this.taskExecutorUtilization = taskExecutorUtilization;
	}

	double getTaskExecutorUtilization() {
		return taskExecutorUtilization;
	}

	@Override
	public AllocationID getAllocationId() {
		return slotInfoDelegate.getAllocationId();
	}

	@Override
	public TaskManagerLocation getTaskManagerLocation() {
		return slotInfoDelegate.getTaskManagerLocation();
	}

	@Override
	public int getPhysicalSlotNumber() {
		return slotInfoDelegate.getPhysicalSlotNumber();
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return slotInfoDelegate.getResourceProfile();
	}

	@Override
	public boolean willBeOccupiedIndefinitely() {
		return slotInfoDelegate.willBeOccupiedIndefinitely();
	}

	public static SlotInfoWithUtilization from(SlotInfo slotInfo, double taskExecutorUtilization) {
		return new SlotInfoWithUtilization(slotInfo, taskExecutorUtilization);
	}
}
