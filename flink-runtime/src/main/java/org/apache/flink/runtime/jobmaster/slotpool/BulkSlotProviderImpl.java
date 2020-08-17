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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BulkSlotProvider}.
 */
public class BulkSlotProviderImpl implements BulkSlotProvider {

	private static final Logger LOG = LoggerFactory.getLogger(BulkSlotProviderImpl.class);

	private final SlotSelectionStrategy slotSelectionStrategy;

	private final SlotPool slotPool;

	private final PhysicalSlotRequestBulkChecker slotRequestBulkChecker;

	public BulkSlotProviderImpl(
			final SlotSelectionStrategy slotSelectionStrategy,
			final SlotPool slotPool,
			final PhysicalSlotRequestBulkChecker slotRequestBulkChecker) {
		this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
		this.slotPool = checkNotNull(slotPool);
		this.slotRequestBulkChecker = checkNotNull(slotRequestBulkChecker);
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
		slotPool.releaseSlot(slotRequestId, cause);
	}

	@Override
	public CompletableFuture<Collection<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
			final Collection<PhysicalSlotRequest> physicalSlotRequests,
			final Time timeout) {
		LOG.debug("Received {} slot requests.", physicalSlotRequests.size());

		final PhysicalSlotRequestBulkImpl slotRequestBulk = createPhysicalSlotRequestBulk(physicalSlotRequests);

		final List<CompletableFuture<PhysicalSlotRequest.Result>> resultFutures = new ArrayList<>(physicalSlotRequests.size());
		for (PhysicalSlotRequest request : physicalSlotRequests) {
			final CompletableFuture<PhysicalSlotRequest.Result> resultFuture =
				allocatePhysicalSlot(request).thenApply(result -> {
					slotRequestBulk.markRequestFulfilled(
						result.getSlotRequestId(),
						result.getPhysicalSlot().getAllocationId());

					return result;
				});
			resultFutures.add(resultFuture);
		}

		slotRequestBulkChecker.schedulePendingRequestBulkTimeoutCheck(slotRequestBulk, timeout);

		return FutureUtils.combineAll(resultFutures);
	}

	private CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(
			final PhysicalSlotRequest physicalSlotRequest) {

		final SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
		final SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
		final ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

		LOG.debug("Received slot request [{}] with resource requirements: {}", slotRequestId, resourceProfile);

		final Optional<PhysicalSlot> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile);

		final CompletableFuture<PhysicalSlot> slotFuture;
		if (availablePhysicalSlot.isPresent()) {
			slotFuture = CompletableFuture.completedFuture(availablePhysicalSlot.get());
		} else {
			slotFuture = requestNewSlot(
				slotRequestId,
				resourceProfile,
				physicalSlotRequest.willSlotBeOccupiedIndefinitely());
		}

		return slotFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
	}

	private Optional<PhysicalSlot> tryAllocateFromAvailable(
			final SlotRequestId slotRequestId,
			final SlotProfile slotProfile) {

		final Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
			slotPool.getAvailableSlotsInformation()
				.stream()
				.map(SlotSelectionStrategy.SlotInfoAndResources::fromSingleSlot)
				.collect(Collectors.toList());

		final Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

		return selectedAvailableSlot.flatMap(
			slotInfoAndLocality -> slotPool.allocateAvailableSlot(
				slotRequestId,
				slotInfoAndLocality.getSlotInfo().getAllocationId())
		);
	}

	private CompletableFuture<PhysicalSlot> requestNewSlot(
			final SlotRequestId slotRequestId,
			final ResourceProfile resourceProfile,
			final boolean willSlotBeOccupiedIndefinitely) {

		if (willSlotBeOccupiedIndefinitely) {
			return slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, null);
		} else {
			slotPool.disableBatchSlotRequestTimeoutCheck();
			return slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile);
		}
	}

	private PhysicalSlotRequestBulkImpl createPhysicalSlotRequestBulk(final Collection<PhysicalSlotRequest> physicalSlotRequests) {
		final PhysicalSlotRequestBulkImpl slotRequestBulk = new PhysicalSlotRequestBulkImpl(
			physicalSlotRequests
				.stream()
				.collect(Collectors.toMap(
					PhysicalSlotRequest::getSlotRequestId,
					r -> r.getSlotProfile().getPhysicalSlotResourceProfile())), this::cancelSlotRequest);
		return slotRequestBulk;
	}
}
