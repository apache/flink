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

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link LeastUtilizationSlotMatchingStrategy}.
 */
public class LeastUtilizationSlotMatchingStrategyTest extends TestLogger {

	@Test
	public void findMatchingSlot_multipleMatchingSlots_returnsSlotWithLeastUtilization() {
		final ResourceProfile requestedResourceProfile = ResourceProfile.fromResources(2.0, 2);

		final TestingTaskManagerSlotInformation leastUtilizedSlot = TestingTaskManagerSlotInformation.newBuilder()
			.setResourceProfile(requestedResourceProfile)
			.build();
		final TestingTaskManagerSlotInformation tooSmallSlot = TestingTaskManagerSlotInformation.newBuilder()
			.setResourceProfile(ResourceProfile.fromResources(1.0, 10))
			.build();
		final TestingTaskManagerSlotInformation alternativeSlot = TestingTaskManagerSlotInformation.newBuilder()
			.setResourceProfile(requestedResourceProfile)
			.build();

		final Collection<TestingTaskManagerSlotInformation> freeSlots = Arrays.asList(
			tooSmallSlot,
			leastUtilizedSlot,
			alternativeSlot);

		Map<InstanceID, Integer> registeredSlotPerTaskExecutor = ImmutableMap.of(
			leastUtilizedSlot.getInstanceId(), 1,
			tooSmallSlot.getInstanceId(), 1,
			alternativeSlot.getInstanceId(), 2);

		final Optional<TestingTaskManagerSlotInformation> matchingSlot = LeastUtilizationSlotMatchingStrategy.INSTANCE.findMatchingSlot(
			requestedResourceProfile,
			freeSlots,
			createRegisteredSlotsLookupFunction(registeredSlotPerTaskExecutor));

		assertTrue(matchingSlot.isPresent());
		assertThat(matchingSlot.get().getSlotId(), is(leastUtilizedSlot.getSlotId()));
	}

	private Function<InstanceID, Integer> createRegisteredSlotsLookupFunction(Map<InstanceID, Integer> registeredSlotPerTaskExecutor) {
		return instanceID -> registeredSlotPerTaskExecutor.getOrDefault(instanceID, 0);
	}
}
