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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

/**
 * Exception indicating that the Flink job with the given job ID has terminated without
 * cancellation.
 */
public class FlinkJobTerminatedWithoutCancellationException extends FlinkException {

    private static final long serialVersionUID = 2294698055059659025L;

    private final JobStatus jobStatus;

    public FlinkJobTerminatedWithoutCancellationException(JobID jobId, JobStatus jobStatus) {
        super(
                String.format(
                        "Flink job (%s) was not canceled, but instead %s.",
                        jobId, assertNotCanceled(jobStatus)));
        this.jobStatus = jobStatus;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    private static JobStatus assertNotCanceled(JobStatus jobStatus) {
        Preconditions.checkState(jobStatus != JobStatus.CANCELED);
        return jobStatus;
    }
}
