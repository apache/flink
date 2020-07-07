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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BulkSlotProvider}.
 */
class BulkSlotProviderImpl implements BulkSlotProvider {

	private static final Logger LOG = LoggerFactory.getLogger(BulkSlotProviderImpl.class);

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	private final SlotSelectionStrategy slotSelectionStrategy;

	private final SlotPool slotPool;

	private final PhysicalSlotRequestBulkChecker slotRequestBulkChecker;

	BulkSlotProviderImpl(final SlotSelectionStrategy slotSelectionStrategy, final SlotPool slotPool) {
		this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
		this.slotPool = checkNotNull(slotPool);

		this.slotRequestBulkChecker = new PhysicalSlotRequestBulkChecker(
			this::getAllSlotInfos,
			SystemClock.getInstance());

		this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
			"Scheduler is not initialized with proper main thread executor. " +
				"Call to BulkSlotProvider.start(...) required.");
	}

	@Override
	public void start(final ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;

		slotPool.disableBatchSlotRequestTimeoutCheck();
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
		componentMainThreadExecutor.assertRunningInMainThread();

		slotPool.releaseSlot(slotRequestId, cause);
	}

	@Override
	public CompletableFuture<Collection<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
			final Collection<PhysicalSlotRequest> physicalSlotRequests,
			final Time timeout) {

		componentMainThreadExecutor.assertRunningInMainThread();

		LOG.debug("Received {} slot requests.", physicalSlotRequests.size());

		final PhysicalSlotRequestBulk slotRequestBulk =
			slotRequestBulkChecker.createPhysicalSlotRequestBulk(physicalSlotRequests);

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

		schedulePendingRequestBulkTimeoutCheck(slotRequestBulk, timeout);

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
			return slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile);
		}
	}

	private void schedulePendingRequestBulkTimeoutCheck(
			final PhysicalSlotRequestBulk slotRequestBulk,
			final Time timeout) {

		componentMainThreadExecutor.schedule(() -> {
			final PhysicalSlotRequestBulkChecker.TimeoutCheckResult result =
				slotRequestBulkChecker.checkPhysicalSlotRequestBulkTimeout(slotRequestBulk, timeout);

			switch (result) {
				case PENDING:
					//re-schedule the timeout check
					schedulePendingRequestBulkTimeoutCheck(slotRequestBulk, timeout);
					break;
				case TIMEOUT:
					timeoutSlotRequestBulk(slotRequestBulk);
					break;
				default: // no action to take
			}
		}, timeout.getSize(), timeout.getUnit());
	}

	private void timeoutSlotRequestBulk(final PhysicalSlotRequestBulk slotRequestBulk) {
		final Exception cause = new TimeoutException("Slot request bulk is not fulfillable!");
		// pending requests must be canceled first otherwise they might be fulfilled by
		// allocated slots released from this bulk
		for (SlotRequestId slotRequestId : slotRequestBulk.getPendingRequests().keySet()) {
			cancelSlotRequest(slotRequestId, cause);
		}
		for (SlotRequestId slotRequestId : slotRequestBulk.getFulfilledRequests().keySet()) {
			cancelSlotRequest(slotRequestId, cause);
		}
	}

	private Set<SlotInfo> getAllSlotInfos() {
		return Stream
			.concat(
				slotPool.getAvailableSlotsInformation().stream(),
				slotPool.getAllocatedSlotsInformation().stream())
			.collect(Collectors.toSet());
	}
}
