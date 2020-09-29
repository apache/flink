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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor.DummyComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.clock.Clock;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link PhysicalSlotRequestBulkChecker}.
 */
public class PhysicalSlotRequestBulkCheckerImpl implements PhysicalSlotRequestBulkChecker {

	private ComponentMainThreadExecutor componentMainThreadExecutor = new DummyComponentMainThreadExecutor(
		"PhysicalSlotRequestBulkCheckerImpl is not initialized with proper main thread executor, " +
		"call to PhysicalSlotRequestBulkCheckerImpl#start is required");

	private final Supplier<Set<SlotInfo>> slotsRetriever;

	private final Clock clock;

	PhysicalSlotRequestBulkCheckerImpl(final Supplier<Set<SlotInfo>> slotsRetriever, final Clock clock) {
		this.slotsRetriever = checkNotNull(slotsRetriever);
		this.clock = checkNotNull(clock);
	}

	@Override
	public void start(final ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	@Override
	public void schedulePendingRequestBulkTimeoutCheck(final PhysicalSlotRequestBulk bulk, Time timeout) {
		PhysicalSlotRequestBulkWithTimestamp bulkWithTimestamp = new PhysicalSlotRequestBulkWithTimestamp(bulk);
		bulkWithTimestamp.markUnfulfillable(clock.relativeTimeMillis());
		schedulePendingRequestBulkWithTimestampCheck(bulkWithTimestamp, timeout);
	}

	private void schedulePendingRequestBulkWithTimestampCheck(
			final PhysicalSlotRequestBulkWithTimestamp bulk,
			final Time timeout) {
		componentMainThreadExecutor.schedule(() -> {
			TimeoutCheckResult result = checkPhysicalSlotRequestBulkTimeout(bulk, timeout);

			switch (result) {
				case PENDING:
					//re-schedule the timeout check
					schedulePendingRequestBulkWithTimestampCheck(bulk, timeout);
					break;
				case TIMEOUT:
					Throwable cancellationCause = new NoResourceAvailableException(
						"Slot request bulk is not fulfillable! Could not allocate the required slot within slot request timeout",
						new TimeoutException("Timeout has occurred: " + timeout));
					bulk.cancel(cancellationCause);
					break;
				case FULFILLED:
				default:
					// no action to take
					break;
			}
		}, timeout.getSize(), timeout.getUnit());
	}

	/**
	 * Check the slot request bulk and timeout its requests if it has been unfulfillable for too long.
	 * @param slotRequestBulk bulk of slot requests
	 * @param slotRequestTimeout indicates how long a pending request can be unfulfillable
	 * @return result of the check, indicating the bulk is fulfilled, still pending, or timed out
	 */
	@VisibleForTesting
	TimeoutCheckResult checkPhysicalSlotRequestBulkTimeout(
			final PhysicalSlotRequestBulkWithTimestamp slotRequestBulk,
			final Time slotRequestTimeout) {

		if (slotRequestBulk.getPendingRequests().isEmpty()) {
			return TimeoutCheckResult.FULFILLED;
		}

		final boolean fulfillable = isSlotRequestBulkFulfillable(slotRequestBulk, slotsRetriever);
		if (fulfillable) {
			slotRequestBulk.markFulfillable();
		} else {
			final long currentTimestamp = clock.relativeTimeMillis();

			slotRequestBulk.markUnfulfillable(currentTimestamp);

			final long unfulfillableSince = slotRequestBulk.getUnfulfillableSince();
			if (unfulfillableSince + slotRequestTimeout.toMilliseconds() <= currentTimestamp) {
				return TimeoutCheckResult.TIMEOUT;
			}
		}

		return TimeoutCheckResult.PENDING;
	}

	/**
	 * Returns whether the given bulk of slot requests are possible to be fulfilled at the same time
	 * with all the reusable slots in the slot pool. A reusable slot means the slot is available or
	 * will not be occupied indefinitely.
	 *
	 * @param slotRequestBulk bulk of slot requests to check
	 * @param slotsRetriever supplies slots to be used for the fulfill-ability check
	 * @return true if the slot requests are possible to be fulfilled, otherwise false
	 */
	@VisibleForTesting
	static boolean isSlotRequestBulkFulfillable(
			final PhysicalSlotRequestBulk slotRequestBulk,
			final Supplier<Set<SlotInfo>> slotsRetriever) {

		final Set<AllocationID> assignedSlots = slotRequestBulk.getAllocationIdsOfFulfilledRequests();
		final Set<SlotInfo> reusableSlots = getReusableSlots(slotsRetriever, assignedSlots);
		return areRequestsFulfillableWithSlots(slotRequestBulk.getPendingRequests(), reusableSlots);
	}

	private static Set<SlotInfo> getReusableSlots(
			final Supplier<Set<SlotInfo>> slotsRetriever,
			final Set<AllocationID> slotsToExclude) {

		return slotsRetriever.get().stream()
			.filter(slotInfo -> !slotInfo.willBeOccupiedIndefinitely())
			.filter(slotInfo -> !slotsToExclude.contains(slotInfo.getAllocationId()))
			.collect(Collectors.toSet());
	}

	/**
	 * Tries to match pending requests to all registered slots (available or allocated).
	 *
	 * <p>NOTE: The complexity of the method is currently quadratic (number of pending requests x number of all slots).
	 */
	private static boolean areRequestsFulfillableWithSlots(
			final Collection<ResourceProfile> requestResourceProfiles,
			final Set<SlotInfo> slots) {

		final Set<SlotInfo> remainingSlots = new HashSet<>(slots);
		for (ResourceProfile requestResourceProfile : requestResourceProfiles) {
			final Optional<SlotInfo> matchedSlot = findMatchingSlotForRequest(requestResourceProfile, remainingSlots);
			if (matchedSlot.isPresent()) {
				remainingSlots.remove(matchedSlot.get());
			} else {
				return false;
			}
		}
		return true;
	}

	private static Optional<SlotInfo> findMatchingSlotForRequest(
			final ResourceProfile requestResourceProfile,
			final Collection<SlotInfo> slots) {

		return slots.stream().filter(slot -> slot.getResourceProfile().isMatching(requestResourceProfile)).findFirst();
	}

	public static PhysicalSlotRequestBulkCheckerImpl createFromSlotPool(final SlotPool slotPool, final Clock clock) {
		return new PhysicalSlotRequestBulkCheckerImpl(() -> getAllSlotInfos(slotPool), clock);
	}

	private static Set<SlotInfo> getAllSlotInfos(SlotPool slotPool) {
		return Stream
			.concat(
				slotPool.getAvailableSlotsInformation().stream(),
				slotPool.getAllocatedSlotsInformation().stream())
			.collect(Collectors.toSet());
	}

	enum TimeoutCheckResult {
		PENDING,

		FULFILLED,

		TIMEOUT
	}
}
