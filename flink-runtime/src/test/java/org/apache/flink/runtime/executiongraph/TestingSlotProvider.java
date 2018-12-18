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
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link SlotProvider} implementation for testing purposes.
 */
final class TestingSlotProvider implements SlotProvider {

	private final ConcurrentMap<SlotRequestId, CompletableFuture<LogicalSlot>> slotFutures;

	private final Function<SlotRequestId, CompletableFuture<LogicalSlot>> slotFutureCreator;

	private volatile Consumer<SlotRequestId> slotCanceller = ignored -> {};

	TestingSlotProvider(Function<SlotRequestId, CompletableFuture<LogicalSlot>> slotFutureCreator) {
		this.slotFutureCreator = slotFutureCreator;
		this.slotFutures = new ConcurrentHashMap<>(4);
	}

	public void setSlotCanceller(Consumer<SlotRequestId> slotCanceller) {
		this.slotCanceller = slotCanceller;
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(SlotRequestId slotRequestId, ScheduledUnit task, boolean allowQueued, SlotProfile slotProfile, Time timeout) {
		Preconditions.checkState(!slotFutures.containsKey(slotRequestId));
		final CompletableFuture<LogicalSlot> slotFuture = slotFutureCreator.apply(slotRequestId);

		slotFutures.put(slotRequestId, slotFuture);

		return slotFuture;
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		final CompletableFuture<LogicalSlot> slotFuture = slotFutures.remove(slotRequestId);
		slotFuture.cancel(false);

		slotCanceller.accept(slotRequestId);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	public void complete(SlotRequestId slotRequestId, LogicalSlot logicalSlot) {
		slotFutures.get(slotRequestId).complete(logicalSlot);
	}
}
