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

import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A ResourceSlot represents a slot located in TaskManager from ResourceManager's view. It has a unique
 * identification and resource profile which we can compare to the resource request.
 */
public class ResourceSlot implements ResourceIDRetrievable {

	private static final long serialVersionUID = -5853720153136840674L;

	/** The unique identification of this slot */
	private final SlotID slotId;

	/** The resource profile of this slot */
	private final ResourceProfile resourceProfile;

	/** Gateway to the TaskExecutor which owns the slot */
	private final TaskExecutorGateway taskExecutorGateway;

	public ResourceSlot(SlotID slotId, ResourceProfile resourceProfile, TaskExecutorGateway taskExecutorGateway) {
		this.slotId = checkNotNull(slotId);
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskExecutorGateway = taskExecutorGateway;
	}

	@Override
	public ResourceID getResourceID() {
		return slotId.getResourceID();
	}

	public SlotID getSlotId() {
		return slotId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public TaskExecutorGateway getTaskExecutorGateway() {
		return taskExecutorGateway;
	}

	/**
	 * Check whether required resource profile can be matched by this slot.
	 *
	 * @param required The required resource profile
	 * @return true if requirement can be matched
	 */
	public boolean isMatchingRequirement(ResourceProfile required) {
		return resourceProfile.isMatching(required);
	}
}
