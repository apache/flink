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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link ExecutionGraphInfoStore} implementation which stores the {@link ArchivedExecutionGraph} in
 * memory.
 */
public class MemoryExecutionGraphInfoStore implements ExecutionGraphInfoStore {

    private final Map<JobID, ExecutionGraphInfo> serializableExecutionGraphInfos = new HashMap<>(4);

    @Override
    public int size() {
        return serializableExecutionGraphInfos.size();
    }

    @Nullable
    @Override
    public ExecutionGraphInfo get(JobID jobId) {
        return serializableExecutionGraphInfos.get(jobId);
    }

    @Override
    public void put(ExecutionGraphInfo serializableExecutionGraphInfo) throws IOException {
        serializableExecutionGraphInfos.put(
                serializableExecutionGraphInfo.getJobId(), serializableExecutionGraphInfo);
    }

    @Override
    public JobsOverview getStoredJobsOverview() {
        Collection<JobStatus> allJobStatus =
                serializableExecutionGraphInfos.values().stream()
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        return JobsOverview.create(allJobStatus);
    }

    @Override
    public Collection<JobDetails> getAvailableJobDetails() {
        return serializableExecutionGraphInfos.values().stream()
                .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public JobDetails getAvailableJobDetails(JobID jobId) {
        final ExecutionGraphInfo archivedExecutionGraphInfo =
                serializableExecutionGraphInfos.get(jobId);

        if (archivedExecutionGraphInfo != null) {
            return JobDetails.createDetailsForJob(
                    archivedExecutionGraphInfo.getArchivedExecutionGraph());
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        serializableExecutionGraphInfos.clear();
    }
}
