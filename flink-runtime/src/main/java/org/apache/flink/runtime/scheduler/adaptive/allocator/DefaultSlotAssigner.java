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
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Simple {@link SlotAssigner} that threats all slots and slot sharing groups equally. */
public class DefaultSlotAssigner implements SlotAssigner {

    @Override
    public Map<SlotSharingGroupId, Set<? extends SlotInfo>> splitSlotsBetweenSlotSharingGroups(
            Collection<? extends SlotInfo> slots, Collection<SlotSharingGroup> groups) {
        final int slotsPerSlotSharingGroup = slots.size() / groups.size();
        if (slotsPerSlotSharingGroup == 0) {
            // => less slots than slot-sharing groups
            return Collections.emptyMap();
        }
        final Iterator<? extends SlotInfo> iterator = slots.iterator();
        final Map<SlotSharingGroupId, Set<? extends SlotInfo>> result = new HashMap<>();
        for (SlotSharingGroup group : groups) {
            final Set<SlotInfo> groupSlots = new HashSet<>();
            result.put(group.getSlotSharingGroupId(), groupSlots);
            for (int i = 0; i < slotsPerSlotSharingGroup; i++) {
                groupSlots.add(iterator.next());
            }
        }
        return result;
    }

    @Override
    public List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignSlots(
            Collection<? extends SlotInfo> slots,
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup> groups) {
        final Iterator<? extends SlotInfo> iterator = slots.iterator();
        final List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assigned =
                new ArrayList<>();
        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroup group : groups) {
            assigned.add(
                    new SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot(
                            group, iterator.next()));
        }
        return assigned;
    }
}
