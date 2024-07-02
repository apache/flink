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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Optional;

/**
 * This class implements a {@link SlotSelectionStrategy} that is based on the tasks balanced
 * strategy.
 */
public class TasksBalancedLocationPreferenceSlotSelectionStrategy
        extends EvenlySpreadOutLocationPreferenceSlotSelectionStrategy {
    public static final Logger LOG =
            LoggerFactory.getLogger(TasksBalancedLocationPreferenceSlotSelectionStrategy.class);

    @Nonnull
    @Override
    protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker,
            @Nonnull LoadableResourceProfile loadableResourceProfile) {

        for (AllocationID allocationId : freeSlotInfoTracker.getAvailableSlots()) {
            SlotInfo candidate = freeSlotInfoTracker.getSlotInfo(allocationId);
            Optional<LoadableResourceProfile> previousLoadableProfile =
                    candidate.getPreviousLoadableResourceProfile();
            if (previousLoadableProfile
                    .map(resourceProfile -> resourceProfile.isMatching(loadableResourceProfile))
                    .orElse(false)) {
                LOG.debug(
                        "Matched slot request {} with {} from available slots.",
                        loadableResourceProfile,
                        candidate);
                return Optional.of(SlotInfoAndLocality.of(candidate, Locality.UNCONSTRAINED));
            }
        }
        return Optional.empty();
    }
}
