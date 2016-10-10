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
package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.mockito.Mockito;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

public class TestingSlotManager extends SlotManager {

	public TestingSlotManager() {
		this(new TestingResourceManagerServices());
	}

	public TestingSlotManager(ResourceManagerServices rmServices) {
		super(rmServices);
	}

	@Override
	protected ResourceSlot chooseSlotToUse(SlotRequest request, Map<SlotID, ResourceSlot> freeSlots) {
		final Iterator<ResourceSlot> slotIterator = freeSlots.values().iterator();
		if (slotIterator.hasNext()) {
			return slotIterator.next();
		} else {
			return null;
		}
	}

	@Override
	protected SlotRequest chooseRequestToFulfill(ResourceSlot offeredSlot, Map<AllocationID, SlotRequest> pendingRequests) {
		final Iterator<SlotRequest> requestIterator = pendingRequests.values().iterator();
		if (requestIterator.hasNext()) {
			return requestIterator.next();
		} else {
			return null;
		}
	}

	private static class TestingResourceManagerServices implements ResourceManagerServices {

		private final UUID leaderID = UUID.randomUUID();

		@Override
		public UUID getLeaderID() {
			return leaderID;
		}

		@Override
		public void allocateResource(ResourceProfile resourceProfile) {

		}

		@Override
		public Executor getAsyncExecutor() {
			return Mockito.mock(Executor.class);
		}

		@Override
		public Executor getMainThreadExecutor() {
			return Mockito.mock(Executor.class);
		}
	}
}
