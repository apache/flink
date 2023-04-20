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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import java.util.Objects;

/** Model to save local slot allocation info. */
public class SlotAllocationSnapshot implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private final SlotID slotID;
    private final JobID jobId;
    private final String jobTargetAddress;
    private final AllocationID allocationId;
    private final ResourceProfile resourceProfile;

    public SlotAllocationSnapshot(
            SlotID slotID,
            JobID jobId,
            String jobTargetAddress,
            AllocationID allocationId,
            ResourceProfile resourceProfile) {
        this.slotID = slotID;
        this.jobId = jobId;
        this.jobTargetAddress = jobTargetAddress;
        this.allocationId = allocationId;
        this.resourceProfile = resourceProfile;
    }

    public SlotID getSlotID() {
        return slotID;
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobTargetAddress() {
        return jobTargetAddress;
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public int getSlotIndex() {
        return slotID.getSlotNumber();
    }

    @Override
    public String toString() {
        return "SlotAllocationSnapshot{"
                + "slotID="
                + slotID
                + ", jobId="
                + jobId
                + ", jobTargetAddress='"
                + jobTargetAddress
                + '\''
                + ", allocationId="
                + allocationId
                + ", resourceProfile="
                + resourceProfile
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotAllocationSnapshot that = (SlotAllocationSnapshot) o;
        return slotID.equals(that.slotID)
                && jobId.equals(that.jobId)
                && jobTargetAddress.equals(that.jobTargetAddress)
                && allocationId.equals(that.allocationId)
                && resourceProfile.equals(that.resourceProfile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slotID, jobId, jobTargetAddress, allocationId, resourceProfile);
    }
}
