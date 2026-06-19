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

package org.apache.flink.table.refresh;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/** Embedded continuous refresh handler of Flink streaming job for materialized table. */
@Internal
public class ContinuousRefreshHandler implements RefreshHandler, Serializable {

    private static final long serialVersionUID = 1L;

    private final String executionTarget;
    private final String clusterId;
    private final String jobId;
    private final @Nullable String restorePath;

    public ContinuousRefreshHandler(String executionTarget, String clusterId, String jobId) {
        this.executionTarget = executionTarget;
        this.clusterId = clusterId;
        this.jobId = jobId;
        this.restorePath = null;
    }

    public ContinuousRefreshHandler(
            String executionTarget, String clusterId, String jobId, String restorePath) {
        this.executionTarget = executionTarget;
        this.clusterId = clusterId;
        this.jobId = jobId;
        this.restorePath = restorePath;
    }

    public String getExecutionTarget() {
        return executionTarget;
    }

    public String getJobId() {
        return jobId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public Optional<String> getRestorePath() {
        return Optional.ofNullable(restorePath);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "{\n executionTarget=%s,\n clusterId=%s,\n jobId=%s%s\n}",
                executionTarget,
                clusterId,
                jobId,
                restorePath == null ? "" : ",\n restorePath=" + restorePath);
    }
}
