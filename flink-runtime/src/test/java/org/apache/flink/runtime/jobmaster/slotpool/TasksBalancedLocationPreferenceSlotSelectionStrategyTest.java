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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TasksBalancedLocationPreferenceSlotSelectionStrategy}. */
class TasksBalancedLocationPreferenceSlotSelectionStrategyTest
        extends SlotSelectionStrategyTestBase {

    private final SlotInfo slotInfo2OfTml1 =
            new SimpleSlotContext(new AllocationID(), tml1, 1, taskManagerGateway, resourceProfile);
    private final SlotInfo slotInfo2OfTml2 =
            new SimpleSlotContext(
                    new AllocationID(), tml2, 2, taskManagerGateway, biggerResourceProfile);
    private final SlotInfo slotInfo2OfTml3 =
            new SimpleSlotContext(new AllocationID(), tml3, 3, taskManagerGateway, resourceProfile);

    @BeforeEach
    void setup() {
        this.selectionStrategy = LocationPreferenceSlotSelectionStrategy.createBalancedTasks();
    }

    private FreeSlotInfoTracker createTestingCandidates() {
        Map<AllocationID, SlotInfo> candidates = getSlotInfosMap();

        candidates.put(slotInfo2OfTml1.getAllocationId(), slotInfo2OfTml1);
        candidates.put(slotInfo2OfTml2.getAllocationId(), slotInfo2OfTml2);
        candidates.put(slotInfo2OfTml3.getAllocationId(), slotInfo2OfTml3);
        return FreeSlotInfoTrackerTestUtils.createDefaultFreeSlotInfoTracker(candidates);
    }

    @Test
    void testBalancedRequestSlotSelectionStrategy() {

        FreeSlotInfoTracker testingCandidates = createTestingCandidates();

        SlotProfile slotProfile1 = createSlotProfile(resourceProfile);
        Optional<SlotSelectionStrategy.SlotInfoAndLocality> slotInfoAndLocality =
                selectionStrategy.selectBestSlotForProfile(testingCandidates, slotProfile1);
        assertThat(slotInfoAndLocality).isEmpty();
        // Calling the method to set previous loading as EMPTY.
        slotInfo2OfTml1.resetLoading();
        // Calling the methods to set previous loading as DefaultLoadingWeight(1f).
        slotInfo2OfTml3.setLoading(new DefaultLoadingWeight(1f));
        slotInfo2OfTml3.resetLoading();

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> slotInfoAndLocality1 =
                selectionStrategy.selectBestSlotForProfile(testingCandidates, slotProfile1);
        assertThat(slotInfoAndLocality1)
                .hasValueSatisfying(
                        slotAndLocality -> {
                            assertThat(slotAndLocality.getSlotInfo()).isEqualTo(slotInfo2OfTml1);
                        });

        Optional<SlotSelectionStrategy.SlotInfoAndLocality> slotInfoAndLocality3 =
                selectionStrategy.selectBestSlotForProfile(
                        testingCandidates,
                        createSlotProfile(resourceProfile, new DefaultLoadingWeight(1f)));
        assertThat(slotInfoAndLocality3)
                .hasValueSatisfying(
                        slotAndLocality -> {
                            assertThat(slotAndLocality.getSlotInfo()).isEqualTo(slotInfo2OfTml3);
                        });
    }

    private SlotProfile createSlotProfile(ResourceProfile resourceProfile) {
        return SlotProfile.priorAllocation(
                ResourceProfile.UNKNOWN,
                resourceProfile.toEmptyLoadable(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptySet());
    }

    private SlotProfile createSlotProfile(
            ResourceProfile resourceProfile, LoadingWeight loadingWeight) {
        return SlotProfile.priorAllocation(
                ResourceProfile.UNKNOWN,
                resourceProfile.toLoadable(loadingWeight),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptySet());
    }
}
