/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A forward group is a set of job vertices connected via forward edges. Parallelisms of all job
 * vertices in the same {@link ForwardGroup} must be the same.
 */
public class ForwardGroup {

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;
    private final Set<JobVertexID> jobVertexIds = new HashSet<>();

    public ForwardGroup(final Set<JobVertex> jobVertices) {
        checkNotNull(jobVertices);

        Set<Integer> configuredParallelisms =
                jobVertices.stream()
                        .filter(
                                jobVertex -> {
                                    jobVertexIds.add(jobVertex.getID());
                                    return jobVertex.getParallelism() > 0;
                                })
                        .map(JobVertex::getParallelism)
                        .collect(Collectors.toSet());

        checkState(configuredParallelisms.size() <= 1);
        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                jobVertices.stream()
                        .map(JobVertex::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a job vertex in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }
    }

    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    public int size() {
        return jobVertexIds.size();
    }

    public Set<JobVertexID> getJobVertexIds() {
        return jobVertexIds;
    }
}
