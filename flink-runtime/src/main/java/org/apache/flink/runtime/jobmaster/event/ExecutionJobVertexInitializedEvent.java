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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class is used to record the initialization info of {@link ExecutionJobVertex}. */
public class ExecutionJobVertexInitializedEvent implements JobEvent {

    private final JobVertexID jobVertexId;

    private final int parallelism;

    // Required by initialization of ExecutionJobVertex.
    private final Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos;

    public ExecutionJobVertexInitializedEvent(
            JobVertexID jobVertexId,
            int parallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos) {
        checkArgument(parallelism > 0);
        this.jobVertexId = checkNotNull(jobVertexId);
        this.parallelism = parallelism;
        this.jobVertexInputInfos = checkNotNull(jobVertexInputInfos);
    }

    public int getParallelism() {
        return parallelism;
    }

    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    public Map<IntermediateDataSetID, JobVertexInputInfo> getJobVertexInputInfos() {
        return jobVertexInputInfos;
    }

    @Override
    public String toString() {
        return "ExecutionJobVertexInitializedEvent("
                + "jobVertexId='"
                + jobVertexId
                + "', parallelism='"
                + parallelism
                + "')";
    }
}
