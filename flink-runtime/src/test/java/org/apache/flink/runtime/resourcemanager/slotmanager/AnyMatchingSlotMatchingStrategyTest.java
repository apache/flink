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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AnyMatchingSlotMatchingStrategy}. */
class AnyMatchingSlotMatchingStrategyTest {

    private final InstanceID instanceId = new InstanceID();

    private TestingTaskManagerSlotInformation largeTaskManagerSlotInformation = null;
    private Collection<TestingTaskManagerSlotInformation> freeSlots = null;

    @BeforeEach
    void setup() {
        final ResourceProfile largeResourceProfile = ResourceProfile.fromResources(10.2, 42);
        final ResourceProfile smallResourceProfile = ResourceProfile.fromResources(1, 1);

        largeTaskManagerSlotInformation =
                TestingTaskManagerSlotInformation.newBuilder()
                        .setInstanceId(instanceId)
                        .setResourceProfile(largeResourceProfile)
                        .build();

        freeSlots =
                Arrays.asList(
                        TestingTaskManagerSlotInformation.newBuilder()
                                .setInstanceId(instanceId)
                                .setResourceProfile(smallResourceProfile)
                                .build(),
                        largeTaskManagerSlotInformation);
    }

    @Test
    void findMatchingSlot_withFulfillableRequest_returnsFulfillingSlot() {
        final Optional<TestingTaskManagerSlotInformation> optionalMatchingSlot =
                AnyMatchingSlotMatchingStrategy.INSTANCE.findMatchingSlot(
                        largeTaskManagerSlotInformation.getResourceProfile(),
                        freeSlots,
                        countSlotsPerInstance(freeSlots));

        assertThat(optionalMatchingSlot)
                .hasValueSatisfying(
                        slot ->
                                assertThat(slot.getSlotId())
                                        .isEqualTo(largeTaskManagerSlotInformation.getSlotId()));
    }

    @Test
    void findMatchingSlot_withUnfulfillableRequest_returnsEmptyResult() {
        final Optional<TestingTaskManagerSlotInformation> optionalMatchingSlot =
                AnyMatchingSlotMatchingStrategy.INSTANCE.findMatchingSlot(
                        ResourceProfile.fromResources(Double.MAX_VALUE, Integer.MAX_VALUE),
                        freeSlots,
                        countSlotsPerInstance(freeSlots));

        assertThat(optionalMatchingSlot).isNotPresent();
    }

    private Function<InstanceID, Integer> countSlotsPerInstance(
            Collection<? extends TestingTaskManagerSlotInformation> freeSlots) {
        return currentInstanceId ->
                (int)
                        freeSlots.stream()
                                .filter(slot -> slot.getInstanceId().equals(currentInstanceId))
                                .count();
    }
}
