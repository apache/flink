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
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

/** The implementation to match slot by latest loading weight. */
public enum LeastLoadingWeightSlotMatchingStrategy implements SlotMatchingStrategy {
    INSTANCE;

    @Override
    public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
            ResourceProfile requestedProfile,
            LoadingWeight loadingWeight,
            Collection<T> freeSlots,
            Function<InstanceID, Integer> numberRegisteredSlotsLookup,
            Function<InstanceID, LoadingWeight> loadingWeightLookup) {

        T taskManagerSlotInformation =
                freeSlots.stream()
                        .filter(
                                taskManagerSlot ->
                                        taskManagerSlot.isMatchingRequirement(requestedProfile))
                        .min(
                                (Comparator<TaskManagerSlotInformation>)
                                        (slot1, slot2) -> {
                                            LoadingWeight loading1 =
                                                    loadingWeightLookup.apply(
                                                            slot1.getInstanceId());
                                            LoadingWeight loading2 =
                                                    loadingWeightLookup.apply(
                                                            slot2.getInstanceId());
                                            return loading1.compareTo(loading2);
                                        })
                        .orElse(null);
        if (taskManagerSlotInformation != null) {
            taskManagerSlotInformation.setLoading(loadingWeight);
        }
        return Optional.ofNullable(taskManagerSlotInformation);
    }
}
