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
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.Map;

/** Utils to create testing {@link FreeSlotInfoTracker}. */
public class FreeSlotInfoTrackerTestUtils {
    /**
     * Create default free slot info tracker for provided slots.
     *
     * @param freeSlots slots to track
     * @return default free slot info tracker
     */
    public static DefaultFreeSlotInfoTracker createDefaultFreeSlotInfoTracker(
            Map<AllocationID, SlotInfo> freeSlots) {
        return new DefaultFreeSlotInfoTracker(
                freeSlots.keySet(),
                freeSlots::get,
                id -> new TestingFreeSlotInfoTracker.TestingFreeSlotInfo(freeSlots.get(id)),
                ignored -> 0d);
    }
}
