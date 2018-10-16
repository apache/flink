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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;

import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of {@link SlotPool}.
 */
public class TestingSlotPool extends SlotPool {

	public TestingSlotPool(RpcService rpcService, JobID jobId, SchedulingStrategy schedulingStrategy) {
		super(rpcService, jobId, schedulingStrategy);
	}

	public CompletableFuture<Integer> getNumberOfAvailableSlots() {
		return callAsync(
			() -> getAvailableSlots().size(),
			TestingUtils.infiniteTime());
	}

	public CompletableFuture<Integer> getNumberOfSharedSlots(SlotSharingGroupId slotSharingGroupId) {
		return callAsync(
			() -> {
				final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

				if (multiTaskSlotManager != null) {
					return multiTaskSlotManager.getResolvedRootSlots().size();
				} else {
					throw new FlinkException("No MultiTaskSlotManager registered under " + slotSharingGroupId + '.');
				}
			},
			TestingUtils.infiniteTime());
	}

	public CompletableFuture<Integer> getNumberOfAvailableSlotsForGroup(SlotSharingGroupId slotSharingGroupId, JobVertexID jobVertexId) {
		return callAsync(
			() -> {
				final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

				if (multiTaskSlotManager != null) {
					int availableSlots = 0;

					for (SlotSharingManager.MultiTaskSlot multiTaskSlot : multiTaskSlotManager.getResolvedRootSlots()) {
						if (!multiTaskSlot.contains(jobVertexId)) {
							availableSlots++;
						}
					}

					return availableSlots;
				} else {
					throw new FlinkException("No MultiTaskSlotmanager registered under " + slotSharingGroupId + '.');
				}
			},
			TestingUtils.infiniteTime());
	}
}
