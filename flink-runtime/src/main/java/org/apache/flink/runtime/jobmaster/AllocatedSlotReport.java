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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The report of currently allocated slots from a given TaskExecutor by a JobMaster. This report is
 * sent periodically to the TaskExecutor in order to reconcile the internal state of slot
 * allocations.
 */
public class AllocatedSlotReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final JobID jobId;

    /** The allocated slots in slot pool. */
    private final Collection<AllocatedSlotInfo> allocatedSlotInfos;

    public AllocatedSlotReport(JobID jobId, Collection<AllocatedSlotInfo> allocatedSlotInfos) {
        this.jobId = checkNotNull(jobId);
        this.allocatedSlotInfos = checkNotNull(allocatedSlotInfos);
    }

    public JobID getJobId() {
        return jobId;
    }

    public Collection<AllocatedSlotInfo> getAllocatedSlotInfos() {
        return Collections.unmodifiableCollection(allocatedSlotInfos);
    }

    @Override
    public String toString() {
        return "AllocatedSlotReport{"
                + "jobId="
                + jobId
                + ", allocatedSlotInfos="
                + allocatedSlotInfos
                + '}';
    }
}
