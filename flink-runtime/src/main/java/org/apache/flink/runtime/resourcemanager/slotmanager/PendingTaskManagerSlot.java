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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a pending task manager slot in the {@link SlotManager}.
 */
public class PendingTaskManagerSlot {

	private final TaskManagerSlotId taskManagerSlotId = TaskManagerSlotId.generate();

	private final ResourceProfile resourceProfile;

	@Nullable
	private PendingSlotRequest pendingSlotRequest;

	public PendingTaskManagerSlot(ResourceProfile resourceProfile) {
		this.resourceProfile = resourceProfile;
	}

	public TaskManagerSlotId getTaskManagerSlotId() {
		return taskManagerSlotId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public void assignPendingSlotRequest(@Nonnull PendingSlotRequest pendingSlotRequestToAssign) {
		Preconditions.checkState(pendingSlotRequest == null);
		pendingSlotRequest = pendingSlotRequestToAssign;
	}

	public void unassignPendingSlotRequest() {
		pendingSlotRequest = null;
	}

	@Nullable
	public PendingSlotRequest getAssignedPendingSlotRequest() {
		return pendingSlotRequest;
	}
}
