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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

class EvenlySpreadOutLocationPreferenceSlotSelectionStrategy
        extends LocationPreferenceSlotSelectionStrategy {
    @Nonnull
    @Override
    protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull Collection<SlotInfoAndResources> availableSlots,
            @Nonnull ResourceProfile resourceProfile) {
        return availableSlots.stream()
                .filter(
                        slotInfoAndResources ->
                                slotInfoAndResources
                                        .getRemainingResources()
                                        .isMatching(resourceProfile))
                .min(Comparator.comparing(SlotInfoAndResources::getTaskExecutorUtilization))
                .map(
                        slotInfoAndResources ->
                                SlotInfoAndLocality.of(
                                        slotInfoAndResources.getSlotInfo(),
                                        Locality.UNCONSTRAINED));
    }

    @Override
    protected double calculateCandidateScore(
            int localWeigh, int hostLocalWeigh, double taskExecutorUtilization) {
        // taskExecutorUtilization in [0, 1] --> only affects choice if localWeigh and
        // hostLocalWeigh
        // between two candidates are equal
        return localWeigh * 20 + hostLocalWeigh * 2 - taskExecutorUtilization;
    }
}
