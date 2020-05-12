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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Strategy to switch between different {@link SlotProvider} allocation strategies.
 */
public abstract class SlotProviderStrategy {

	protected final SlotProvider slotProvider;

	SlotProviderStrategy(SlotProvider slotProvider) {
		this.slotProvider = Preconditions.checkNotNull(slotProvider);
	}

	/**
	 * Allocating slot with specific requirement.
	 *
	 * @param slotRequestId identifying the slot request
	 * @param scheduledUnit The task to allocate the slot for
	 * @param slotProfile profile of the requested slot
	 * @return The future of the allocation
	 */
	public abstract CompletableFuture<LogicalSlot> allocateSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile);

	/**
	 * Cancels the slot request with the given {@link SlotRequestId} and {@link SlotSharingGroupId}.
	 *
	 * @param slotRequestId identifying the slot request to cancel
	 * @param slotSharingGroupId identifying the slot request to cancel
	 * @param cause of the cancellation
	 */
	public void cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {
		slotProvider.cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
	}

	public static SlotProviderStrategy from(
		ScheduleMode scheduleMode,
		SlotProvider slotProvider,
		Time allocationTimeout) {

		switch (scheduleMode) {
			case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
				return new BatchSlotProviderStrategy(slotProvider);
			case LAZY_FROM_SOURCES:
			case EAGER:
				return new NormalSlotProviderStrategy(slotProvider, allocationTimeout);
			default:
				throw new IllegalArgumentException(String.format("Unknown scheduling mode: %s", scheduleMode));
		}
	}

	SlotProvider asSlotProvider() {
		return slotProvider;
	}

	static class BatchSlotProviderStrategy extends SlotProviderStrategy {

		BatchSlotProviderStrategy(SlotProvider slotProvider) {
			super(slotProvider);
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile) {
			return slotProvider.allocateBatchSlot(slotRequestId, scheduledUnit, slotProfile);
		}
	}

	static class NormalSlotProviderStrategy extends SlotProviderStrategy {
		private final Time allocationTimeout;

		NormalSlotProviderStrategy(SlotProvider slotProvider, Time allocationTimeout) {
			super(slotProvider);
			this.allocationTimeout = Preconditions.checkNotNull(allocationTimeout);
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile) {
			return slotProvider.allocateSlot(slotRequestId, scheduledUnit, slotProfile, allocationTimeout);
		}
	}
}
