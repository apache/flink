/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builder for the {@link JobGraph}. */
public class JobGraphBuilder {

    private final JobType jobType;

    private final List<JobVertex> jobVertices = new ArrayList<>();

    private final Map<String, DistributedCache.DistributedCacheEntry> userArtifacts =
            new HashMap<>();

    private final List<URL> classpaths = new ArrayList<>();

    private String jobName = "Unnamed job";

    @Nullable private JobID jobId = null;

    @Nullable private SerializedValue<ExecutionConfig> serializedExecutionConfig = null;

    @Nullable private JobCheckpointingSettings jobCheckpointingSettings = null;

    @Nullable private SavepointRestoreSettings savepointRestoreSettings = null;

    private JobGraphBuilder(JobType jobType) {
        this.jobType = jobType;
    }

    public JobGraphBuilder setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public JobGraphBuilder addJobVertices(Collection<? extends JobVertex> jobVerticesToAdd) {
        jobVertices.addAll(jobVerticesToAdd);
        return this;
    }

    public JobGraphBuilder addJobVertex(JobVertex jobVertex) {
        return addJobVertices(Collections.singleton(jobVertex));
    }

    public JobGraphBuilder setJobId(JobID jobId) {
        this.jobId = jobId;
        return this;
    }

    public JobGraphBuilder setExecutionConfig(ExecutionConfig newExecutionConfig)
            throws IOException {
        this.serializedExecutionConfig = new SerializedValue<ExecutionConfig>(newExecutionConfig);
        return this;
    }

    public JobGraphBuilder addUserArtifacts(
            Map<String, DistributedCache.DistributedCacheEntry> newUserArtifacts) {
        userArtifacts.putAll(newUserArtifacts);
        return this;
    }

    public JobGraphBuilder setJobCheckpointingSettings(
            JobCheckpointingSettings newJobCheckpointingSettings) {
        this.jobCheckpointingSettings = newJobCheckpointingSettings;
        return this;
    }

    public JobGraphBuilder setSavepointRestoreSettings(
            SavepointRestoreSettings newSavepointRestoreSettings) {
        savepointRestoreSettings = newSavepointRestoreSettings;
        return this;
    }

    public JobGraphBuilder addClasspaths(Collection<URL> additionalClasspaths) {
        classpaths.addAll(additionalClasspaths);
        return this;
    }

    public JobGraph build() {
        final JobGraph jobGraph =
                new JobGraph(jobId, jobName, jobVertices.toArray(new JobVertex[0]));

        jobGraph.setJobType(jobType);

        if (serializedExecutionConfig != null) {
            jobGraph.setSerializedExecutionConfig(serializedExecutionConfig);
        }

        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                userArtifacts.entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        if (jobCheckpointingSettings != null) {
            jobGraph.setSnapshotSettings(jobCheckpointingSettings);
        }

        if (savepointRestoreSettings != null) {
            jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
        }

        if (!classpaths.isEmpty()) {
            jobGraph.setClasspaths(classpaths);
        }

        return jobGraph;
    }

    public static JobGraphBuilder newStreamingJobGraphBuilder() {
        return new JobGraphBuilder(JobType.STREAMING);
    }

    public static JobGraphBuilder newBatchJobGraphBuilder() {
        return new JobGraphBuilder(JobType.BATCH);
    }
}
