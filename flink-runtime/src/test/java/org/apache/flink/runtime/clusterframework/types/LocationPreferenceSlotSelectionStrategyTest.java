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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests for {@link LocationPreferenceSlotSelectionStrategy}.
 */
public class LocationPreferenceSlotSelectionStrategyTest extends SlotSelectionStrategyTestBase {

	public LocationPreferenceSlotSelectionStrategyTest() {
		super(LocationPreferenceSlotSelectionStrategy.createDefault());
	}

	protected LocationPreferenceSlotSelectionStrategyTest(SlotSelectionStrategy slotSelectionStrategy) {
		super(slotSelectionStrategy);
	}

	@Test
	public void testPhysicalSlotResourceProfileRespected() {

		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			biggerResourceProfile,
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptySet());

		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);
		Assert.assertTrue(match.get().getSlotInfo().getResourceProfile().isMatching(slotProfile.getPhysicalSlotResourceProfile()));

		ResourceProfile evenBiggerResourceProfile = new ResourceProfile(
			biggerResourceProfile.getCpuCores().getValue().doubleValue() + 1.0,
			resourceProfile.getTaskHeapMemory().getMebiBytes());
		slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			evenBiggerResourceProfile,
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptySet());

		match = runMatching(slotProfile);
		Assert.assertFalse(match.isPresent());
	}

	@Test
	public void matchNoRequirements() {

		SlotProfile slotProfile = SlotProfile.noRequirements();
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertTrue(
				candidates.stream()
						.map(SlotSelectionStrategy.SlotInfoAndResources::getSlotInfo)
						.collect(Collectors.toList())
						.contains(match.get().getSlotInfo()));
	}

	@Test
	public void matchPreferredLocationNotAvailable() {

		SlotProfile slotProfile = SlotProfile.preferredLocality(resourceProfile, Collections.singletonList(tmlX));
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertTrue(
				candidates.stream()
						.map(SlotSelectionStrategy.SlotInfoAndResources::getSlotInfo)
						.collect(Collectors.toList())
						.contains(match.get().getSlotInfo()));
	}

	@Test
	public void matchPreferredLocation() {

		SlotProfile slotProfile = SlotProfile.preferredLocality(resourceProfile, Collections.singletonList(tml2));
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo2, match.get().getSlotInfo());

		slotProfile = SlotProfile.preferredLocality(resourceProfile, Arrays.asList(tmlX, tml4));
		match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo4, match.get().getSlotInfo());

		slotProfile = SlotProfile.preferredLocality(resourceProfile, Arrays.asList(tml3, tml1, tml3, tmlX));
		match = runMatching(slotProfile);

		Assert.assertEquals(slotInfo3, match.get().getSlotInfo());
	}

	@Test
	public void matchPreviousLocationAvailableButAlsoBlacklisted() {
		HashSet<AllocationID> blacklisted = new HashSet<>(4);
		blacklisted.add(aid1);
		blacklisted.add(aid2);
		blacklisted.add(aid3);
		blacklisted.add(aid4);
		SlotProfile slotProfile = SlotProfile.priorAllocation(
			resourceProfile,
			resourceProfile,
			Collections.singletonList(tml3),
			Collections.singletonList(aid3),
			blacklisted);
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		// available previous allocation should override blacklisting
		Assert.assertEquals(slotInfo3, match.get().getSlotInfo());
	}
}
