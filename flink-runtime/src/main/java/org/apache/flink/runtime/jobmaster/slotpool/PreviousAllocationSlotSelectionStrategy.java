/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * This class implements a {@link SlotSelectionStrategy} that is based on previous allocations and
 * falls back to using location preference hints if there is no previous allocation.
 */
public enum PreviousAllocationSlotSelectionStrategy implements SlotSelectionStrategy {

	INSTANCE;

	@Override
	public Optional<SlotInfoAndLocality> selectBestSlotForProfile(
		@Nonnull Collection<? extends SlotInfo> availableSlots,
		@Nonnull SlotProfile slotProfile) {

		Collection<AllocationID> priorAllocations = slotProfile.getPreferredAllocations();

		// First, if there was a prior allocation try to schedule to the same/old slot
		if (!priorAllocations.isEmpty()) {
			for (SlotInfo availableSlot : availableSlots) {
				if (priorAllocations.contains(availableSlot.getAllocationId())) {
					return Optional.of(
						SlotInfoAndLocality.of(availableSlot, Locality.LOCAL));
				}
			}
		}

		// Second, select based on location preference, excluding blacklisted allocations
		Set<AllocationID> blackListedAllocations = slotProfile.getPreviousExecutionGraphAllocations();
		Collection<? extends SlotInfo> availableAndAllowedSlots = computeWithoutBlacklistedSlots(availableSlots, blackListedAllocations);
		return LocationPreferenceSlotSelectionStrategy.INSTANCE.selectBestSlotForProfile(availableAndAllowedSlots, slotProfile);
	}

	@Nonnull
	private Collection<SlotInfo> computeWithoutBlacklistedSlots(
		@Nonnull Collection<? extends SlotInfo> availableSlots,
		@Nonnull Set<AllocationID> blacklistedAllocations) {

		if (blacklistedAllocations.isEmpty()) {
			return Collections.unmodifiableCollection(availableSlots);
		}

		ArrayList<SlotInfo> availableAndAllowedSlots = new ArrayList<>(availableSlots.size());
		for (SlotInfo availableSlot : availableSlots) {
			if (!blacklistedAllocations.contains(availableSlot.getAllocationId())) {
				availableAndAllowedSlots.add(availableSlot);
			}
		}

		return availableAndAllowedSlots;
	}
}
