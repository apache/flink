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

import java.util.Collection;
import java.util.Set;

class PhysicalSlotRequestBulkWithTimestamp implements PhysicalSlotRequestBulk {
    private final PhysicalSlotRequestBulk physicalSlotRequestBulk;

    private long unfulfillableTimestamp = Long.MAX_VALUE;

    PhysicalSlotRequestBulkWithTimestamp(PhysicalSlotRequestBulk physicalSlotRequestBulk) {
        this.physicalSlotRequestBulk = physicalSlotRequestBulk;
    }

    void markFulfillable() {
        unfulfillableTimestamp = Long.MAX_VALUE;
    }

    void markUnfulfillable(final long currentTimestamp) {
        if (isFulfillable()) {
            unfulfillableTimestamp = currentTimestamp;
        }
    }

    long getUnfulfillableSince() {
        return unfulfillableTimestamp;
    }

    private boolean isFulfillable() {
        return unfulfillableTimestamp == Long.MAX_VALUE;
    }

    @Override
    public Collection<ResourceProfile> getPendingRequests() {
        return physicalSlotRequestBulk.getPendingRequests();
    }

    @Override
    public Set<AllocationID> getAllocationIdsOfFulfilledRequests() {
        return physicalSlotRequestBulk.getAllocationIdsOfFulfilledRequests();
    }

    @Override
    public void cancel(Throwable cause) {
        physicalSlotRequestBulk.cancel(cause);
    }
}
