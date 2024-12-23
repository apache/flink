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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** The allocator util class. */
class AllocatorUtil {

    private AllocatorUtil() {}

    static Map<SlotSharingGroupId, SlotSharingSlotAllocator.SlotSharingGroupMetaInfo>
            getSlotSharingGroupMetaInfos(@Nonnull JobInformation jobInformation) {
        return SlotSharingSlotAllocator.SlotSharingGroupMetaInfo.from(jobInformation.getVertices());
    }

    static int getMinimalRequiredSlots(
            @Nonnull
                    Map<SlotSharingGroupId, SlotSharingSlotAllocator.SlotSharingGroupMetaInfo>
                            slotSharingGroupMetaInfos) {
        return slotSharingGroupMetaInfos.values().stream()
                .map(SlotSharingSlotAllocator.SlotSharingGroupMetaInfo::getMaxLowerBound)
                .reduce(0, Integer::sum);
    }

    static void checkMinimalRequiredSlots(
            @Nonnull JobInformation jobInformation,
            @Nonnull Collection<? extends SlotInfo> freeSlots) {
        final int minimumRequiredSlots =
                getMinimalRequiredSlots(getSlotSharingGroupMetaInfos(jobInformation));
        checkState(
                freeSlots.size() >= minimumRequiredSlots,
                "Not enough slots to allocate all the execution slot sharing groups (have: %s, need: %s)",
                freeSlots.size(),
                minimumRequiredSlots);
    }
}
