/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/** Test {@link SlotInfo} implementation. */
class TestSlotInfo implements SlotInfo {

    private final AllocationID allocationId = new AllocationID();

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return new LocalTaskManagerLocation();
    }

    @Override
    public int getPhysicalSlotNumber() {
        return 0;
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return ResourceProfile.ANY;
    }

    @Override
    public boolean willBeOccupiedIndefinitely() {
        return false;
    }
}
