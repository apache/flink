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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This describes the requirement of the slot, mainly used by JobManager requesting slot from
 * ResourceManager.
 */
public class SlotRequest implements Serializable {

    private static final long serialVersionUID = -6586877187990445986L;

    /** The JobID of the slot requested for */
    private final JobID jobId;

    /** The unique identification of this request */
    private final AllocationID allocationId;

    /** The resource profile of the required slot */
    private final ResourceProfile resourceProfile;

    /** Address of the emitting job manager */
    private final String targetAddress;

    public SlotRequest(
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            String targetAddress) {
        this.jobId = checkNotNull(jobId);
        this.allocationId = checkNotNull(allocationId);
        this.resourceProfile = checkNotNull(resourceProfile);
        this.targetAddress = checkNotNull(targetAddress);
    }

    /**
     * Get the JobID of the slot requested for.
     *
     * @return The job id
     */
    public JobID getJobId() {
        return jobId;
    }

    /**
     * Get the unique identification of this request
     *
     * @return the allocation id
     */
    public AllocationID getAllocationId() {
        return allocationId;
    }

    /**
     * Get the resource profile of the desired slot
     *
     * @return The resource profile
     */
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public String getTargetAddress() {
        return targetAddress;
    }
}
