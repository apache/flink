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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ErrorInfo;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * {@code ExceptionHistoryEntry} collects information about a single failure that triggered the
 * application's failure.
 */
public class ApplicationExceptionHistoryEntry extends ErrorInfo {

    private static final long serialVersionUID = -3855285510064263702L;

    /**
     * The ID of the job that caused the failure.
     *
     * <p>This field is null if the failure was not caused by a job.
     */
    @Nullable private final JobID jobId;

    public ApplicationExceptionHistoryEntry(
            Throwable cause, long timestamp, @Nullable JobID jobId) {
        super(cause, timestamp);
        this.jobId = jobId;
    }

    public Optional<JobID> getJobId() {
        return Optional.ofNullable(jobId);
    }
}
