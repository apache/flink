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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

import javax.annotation.Nullable;

/** Default implementation for {@link JobExecutionStatusEvent}. */
@Internal
public class DefaultJobExecutionStatusEvent implements JobExecutionStatusEvent {
    private final JobID jobId;
    private final String jobName;
    private final JobStatus oldStatus;
    private final JobStatus newStatus;
    @Nullable private final Throwable cause;

    public DefaultJobExecutionStatusEvent(
            JobID jobId,
            String jobName,
            JobStatus oldStatus,
            JobStatus newStatus,
            @Nullable Throwable cause) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
        this.cause = cause;
    }

    @Override
    public JobStatus oldStatus() {
        return oldStatus;
    }

    @Override
    public JobStatus newStatus() {
        return newStatus;
    }

    @Nullable
    @Override
    public Throwable exception() {
        return cause;
    }

    @Override
    public JobID jobId() {
        return jobId;
    }

    @Override
    public String jobName() {
        return jobName;
    }
}
