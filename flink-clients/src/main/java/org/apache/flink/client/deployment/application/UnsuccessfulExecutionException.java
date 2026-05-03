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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmaster.JobResult;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Exception that signals the failure of a job with a given {@link JobStatus}. */
@Internal
public class UnsuccessfulExecutionException extends JobExecutionException {

    @Nullable private final JobStatus status;

    public UnsuccessfulExecutionException(
            final JobID jobID,
            @Nullable final JobStatus status,
            final String message,
            final Throwable cause) {
        super(jobID, message, cause);
        this.status = status;
    }

    public Optional<JobStatus> getStatus() {
        return Optional.ofNullable(status);
    }

    public static UnsuccessfulExecutionException fromJobResult(
            final JobResult result, final ClassLoader userClassLoader) {

        checkState(result != null && !result.isSuccess());
        checkNotNull(userClassLoader);

        // We do this to uniformize the behavior of the "ATTACHED" and "DETACHED"
        // in application mode, while maintaining the expected exceptions thrown in case
        // of a failed job execution.

        try {
            result.toJobExecutionResult(userClassLoader);
            throw new IllegalStateException(
                    "No exception thrown although the job execution was not successful.");

        } catch (Throwable t) {

            final JobID jobID = result.getJobId();
            final JobStatus status = result.getJobStatus().orElse(null);

            return status == JobStatus.CANCELED || status == JobStatus.FAILED
                    ? new UnsuccessfulExecutionException(
                            jobID, status, "Job Status: " + status.name(), t)
                    : new UnsuccessfulExecutionException(
                            jobID, null, "Job failed for unknown reason.", t);
        }
    }
}
