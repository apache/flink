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

import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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

		ResourceProfile evenBiggerResourceProfile = ResourceProfile.fromResources(
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

		Assert.assertTrue(match.isPresent());
		final SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality = match.get();
		assertThat(candidates, hasItem(withSlotInfo(slotInfoAndLocality.getSlotInfo())));
		assertThat(slotInfoAndLocality, hasLocality(Locality.UNCONSTRAINED));
	}

	@Test
	public void returnsHostLocalMatchingIfExactTMLocationCannotBeFulfilled() {

		SlotProfile slotProfile = SlotProfile.preferredLocality(resourceProfile, Collections.singletonList(tmlX));
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertTrue(match.isPresent());
		final SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality = match.get();
		final SlotInfo slotInfo = slotInfoAndLocality.getSlotInfo();
		assertThat(candidates, hasItem(withSlotInfo(slotInfo)));
		assertThat(slotInfoAndLocality, hasLocality(Locality.HOST_LOCAL));
	}

	private Matcher<SlotSelectionStrategy.SlotInfoAndLocality> hasLocality(Locality locality) {
		return new LocalityFeatureMatcher(locality);
	}

	private Matcher<SlotSelectionStrategy.SlotInfoAndResources> withSlotInfo(SlotInfo slotInfo) {
		return new SlotInfoFeatureMatcher(slotInfo);
	}

	@Test
	public void returnsNonLocalMatchingIfResourceProfileCanBeFulfilledButNotTheTMLocationPreferences() throws Exception {
		final InetAddress nonHostLocalInetAddress = InetAddress.getByAddress(new byte[]{10, 0, 0, 24});
		final TaskManagerLocation nonLocalTm = new TaskManagerLocation(new ResourceID("non-local-tm"), nonHostLocalInetAddress, 42);
		SlotProfile slotProfile = SlotProfile.preferredLocality(resourceProfile, Collections.singletonList(nonLocalTm));
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> match = runMatching(slotProfile);

		Assert.assertTrue(match.isPresent());
		final SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality = match.get();
		assertThat(candidates, hasItem(withSlotInfo(slotInfoAndLocality.getSlotInfo())));
		assertThat(slotInfoAndLocality, hasLocality(Locality.NON_LOCAL));
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

	private static class SlotInfoFeatureMatcher extends FeatureMatcher<SlotSelectionStrategy.SlotInfoAndResources, SlotInfo> {
		SlotInfoFeatureMatcher(SlotInfo slotInfo) {
			super(is(slotInfo), "Slot info of a SlotInfoAndResources instance", "slotInfo");
		}

		@Override
		protected SlotInfo featureValueOf(SlotSelectionStrategy.SlotInfoAndResources actual) {
			return actual.getSlotInfo();
		}
	}

	private static class LocalityFeatureMatcher extends FeatureMatcher<SlotSelectionStrategy.SlotInfoAndLocality, Locality> {
		LocalityFeatureMatcher(Locality locality) {
			super(is(locality), "Locality of SlotInfoAndLocality instance", "locality");
		}

		@Override
		protected Locality featureValueOf(SlotSelectionStrategy.SlotInfoAndLocality actual) {
			return actual.getLocality();
		}
	}
}
