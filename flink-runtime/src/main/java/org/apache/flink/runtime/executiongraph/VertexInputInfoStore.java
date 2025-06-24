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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A store contains all the {@link JobVertexInputInfo}s. Note that if a vertex has multiple job
 * edges connecting to the same intermediate result, their {@link DistributionPattern} must be the
 * same and therefore the {@link JobVertexInputInfo} will be the same.
 */
public class VertexInputInfoStore {

    private final Map<JobVertexID, Map<IntermediateDataSetID, JobVertexInputInfo>>
            jobVertexInputInfos = new HashMap<>();

    /**
     * Put a {@link JobVertexInputInfo}.
     *
     * @param jobVertexId the job vertex id
     * @param resultId the intermediate result id
     * @param info the {@link JobVertexInputInfo} to put
     */
    public void put(
            JobVertexID jobVertexId, IntermediateDataSetID resultId, JobVertexInputInfo info) {
        checkNotNull(jobVertexId);
        checkNotNull(resultId);
        checkNotNull(info);

        jobVertexInputInfos.compute(
                jobVertexId,
                (ignored, inputInfos) -> {
                    if (inputInfos == null) {
                        inputInfos = new HashMap<>();
                    }

                    inputInfos.putIfAbsent(resultId, info);
                    return inputInfos;
                });
    }

    /**
     * Get a {@link JobVertexInputInfo}.
     *
     * @param jobVertexId the job vertex id
     * @param resultId the intermediate result id
     * @return the {@link JobVertexInputInfo} identified by the job vertex id and intermediate
     *     result id
     */
    public JobVertexInputInfo get(JobVertexID jobVertexId, IntermediateDataSetID resultId) {
        checkNotNull(jobVertexId);
        checkNotNull(resultId);
        return checkNotNull(jobVertexInputInfos.get(jobVertexId).get(resultId));
    }
}
