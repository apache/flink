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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;

/**
 * Testing implementation of {@link TaskManagerSlotInformation}.
 */
public final class TestingTaskManagerSlotInformation implements TaskManagerSlotInformation {

	private final SlotID slotId;
	private final InstanceID instanceId;
	private final ResourceProfile resourceProfile;

	private TestingTaskManagerSlotInformation(SlotID slotId, InstanceID instanceId, ResourceProfile resourceProfile) {
		this.slotId = slotId;
		this.instanceId = instanceId;
		this.resourceProfile = resourceProfile;
	}

	@Override
	public SlotID getSlotId() {
		return slotId;
	}

	@Override
	public InstanceID getInstanceId() {
		return instanceId;
	}

	@Override
	public boolean isMatchingRequirement(ResourceProfile required) {
		return resourceProfile.isMatching(required);
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	static class Builder {
		private SlotID slotId = new SlotID(ResourceID.generate(), 0);
		private InstanceID instanceId = new InstanceID();
		private ResourceProfile resourceProfile = ResourceProfile.ANY;

		public Builder setInstanceId(InstanceID instanceId) {
			this.instanceId = instanceId;
			return this;
		}

		public Builder setResourceProfile(ResourceProfile resourceProfile) {
			this.resourceProfile = resourceProfile;
			return this;
		}

		public Builder setSlotId(SlotID slotId) {
			this.slotId = slotId;
			return this;
		}

		public TestingTaskManagerSlotInformation build() {
			return new TestingTaskManagerSlotInformation(slotId, instanceId, resourceProfile);
		}
	}
}
