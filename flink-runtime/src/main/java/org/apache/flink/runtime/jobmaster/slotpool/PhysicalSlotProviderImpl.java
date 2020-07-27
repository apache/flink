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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
	private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

	private final SlotSelectionStrategy slotSelectionStrategy;

	private final SlotPool slotPool;

	PhysicalSlotProviderImpl(SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
		this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
		this.slotPool = checkNotNull(slotPool);
		slotPool.disableBatchSlotRequestTimeoutCheck();
	}

	@Override
	public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest) {
		SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
		SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
		ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

		LOG.debug("Received slot request [{}] with resource requirements: {}", slotRequestId, resourceProfile);

		Optional<PhysicalSlot> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile);

		CompletableFuture<PhysicalSlot> slotFuture;
		slotFuture = availablePhysicalSlot
			.map(CompletableFuture::completedFuture)
			.orElseGet(() -> requestNewSlot(
				slotRequestId,
				resourceProfile,
				physicalSlotRequest.willSlotBeOccupiedIndefinitely()));

		return slotFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
	}

	private Optional<PhysicalSlot> tryAllocateFromAvailable(SlotRequestId slotRequestId, SlotProfile slotProfile) {
		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
			slotPool.getAvailableSlotsInformation()
				.stream()
				.map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot)
				.collect(Collectors.toList());

		Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

		return selectedAvailableSlot.flatMap(
			slotInfoAndLocality -> slotPool.allocateAvailableSlot(
				slotRequestId,
				slotInfoAndLocality.getSlotInfo().getAllocationId())
		);
	}

	private CompletableFuture<PhysicalSlot> requestNewSlot(
			SlotRequestId slotRequestId,
			ResourceProfile resourceProfile,
			boolean willSlotBeOccupiedIndefinitely) {
		return willSlotBeOccupiedIndefinitely ?
			slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, null) :
			slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile);
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
		slotPool.releaseSlot(slotRequestId, cause);
	}
}
