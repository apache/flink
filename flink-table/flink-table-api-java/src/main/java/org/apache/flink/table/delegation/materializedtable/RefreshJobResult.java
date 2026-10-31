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

package org.apache.flink.table.delegation.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The result of submitting a materialized table refresh job, describing where the job was deployed
 * and how to identify it. It optionally carries the client returned at submission.
 */
@Internal
public final class RefreshJobResult {

    private final RefreshJobTarget target;
    private final String jobId;
    @Nullable private final JobClient jobClient;

    public RefreshJobResult(RefreshJobTarget target, String jobId, @Nullable JobClient jobClient) {
        this.target = checkNotNull(target);
        this.jobId = checkNotNull(jobId);
        this.jobClient = jobClient;
        checkArgument(
                jobClient == null || jobClient.getJobID().toHexString().equals(jobId),
                "The job client controls job %s, not refresh job %s.",
                jobClient == null ? null : jobClient.getJobID(),
                jobId);
    }

    public RefreshJobTarget getTarget() {
        return target;
    }

    public String getJobId() {
        return jobId;
    }

    public Optional<JobClient> getJobClient() {
        return Optional.ofNullable(jobClient);
    }
}
