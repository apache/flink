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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.Preconditions;

/** Represents a pending task manager in the {@link SlotManager}. */
public class PendingTaskManager {
    private final PendingTaskManagerId pendingTaskManagerId;
    private final ResourceProfile totalResourceProfile;
    private final ResourceProfile defaultSlotResourceProfile;

    public PendingTaskManager(
            PendingTaskManagerId pendingTaskManagerId,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
        this.totalResourceProfile = Preconditions.checkNotNull(totalResourceProfile);
        this.pendingTaskManagerId = Preconditions.checkNotNull(pendingTaskManagerId);
    }

    public ResourceProfile getTotalResourceProfile() {
        return totalResourceProfile;
    }

    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    public PendingTaskManagerId getPendingTaskManagerId() {
        return pendingTaskManagerId;
    }

    @Override
    public String toString() {
        return String.format(
                "PendingTaskManager{id=%s, totalResourceProfile=%s, defaultSlotResourceProfile=%s}",
                pendingTaskManagerId, totalResourceProfile, defaultSlotResourceProfile);
    }
}
