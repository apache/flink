/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AnyMatchingSlotMatchingStrategy}.
 */
public class AnyMatchingSlotMatchingStrategyTest extends TestLogger {

	private final InstanceID instanceId = new InstanceID();

	private TestingTaskManagerSlotInformation largeTaskManagerSlotInformation = null;
	private Collection<TestingTaskManagerSlotInformation> freeSlots = null;

	@Before
	public void setup() {
		final ResourceProfile largeResourceProfile = new ResourceProfile(10.2 , 42);
		final ResourceProfile smallResourceProfile = new ResourceProfile(1 , 1);

		largeTaskManagerSlotInformation = TestingTaskManagerSlotInformation.newBuilder()
			.setInstanceId(instanceId)
			.setResourceProfile(largeResourceProfile)
			.build();

		freeSlots = Arrays.asList(
			TestingTaskManagerSlotInformation.newBuilder()
				.setInstanceId(instanceId)
				.setResourceProfile(smallResourceProfile)
				.build(),
			largeTaskManagerSlotInformation);
	}

	@Test
	public void findMatchingSlot_withFulfillableRequest_returnsFulfillingSlot() {
		final Optional<TestingTaskManagerSlotInformation> optionalMatchingSlot = AnyMatchingSlotMatchingStrategy.INSTANCE.findMatchingSlot(
			largeTaskManagerSlotInformation.getResourceProfile(),
			freeSlots,
			countSlotsPerInstance(freeSlots));

		assertTrue(optionalMatchingSlot.isPresent());
		assertThat(optionalMatchingSlot.get().getSlotId(), is(largeTaskManagerSlotInformation.getSlotId()));
	}

	@Test
	public void findMatchingSlot_withUnfulfillableRequest_returnsEmptyResult() {
		final Optional<TestingTaskManagerSlotInformation> optionalMatchingSlot = AnyMatchingSlotMatchingStrategy.INSTANCE.findMatchingSlot(
			new ResourceProfile(Double.MAX_VALUE, Integer.MAX_VALUE),
			freeSlots,
			countSlotsPerInstance(freeSlots));

		assertFalse(optionalMatchingSlot.isPresent());
	}

	private Function<InstanceID, Integer> countSlotsPerInstance(Collection<? extends TestingTaskManagerSlotInformation> freeSlots) {
		return currentInstanceId -> (int) freeSlots.stream().filter(slot -> slot.getInstanceId().equals(currentInstanceId)).count();
	}

}
