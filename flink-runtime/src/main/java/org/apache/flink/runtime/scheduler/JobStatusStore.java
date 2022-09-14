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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobStatusProvider;

/** Listens for and exposes the current job state and state timestamps. */
public class JobStatusStore implements JobStatusListener, JobStatusProvider {

    private final long[] stateTimestamps = new long[JobStatus.values().length];
    private JobStatus jobStatus = JobStatus.INITIALIZING;

    public JobStatusStore(long initializationTimestamp) {
        stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        jobStatus = newJobStatus;
        stateTimestamps[jobStatus.ordinal()] = timestamp;
    }

    @Override
    public JobStatus getState() {
        return jobStatus;
    }

    @Override
    public long getStatusTimestamp(JobStatus status) {
        return stateTimestamps[status.ordinal()];
    }
}
