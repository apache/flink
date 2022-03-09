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

package org.apache.flink.runtime.scheduler.adaptivebatch.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

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

    private final Set<JobVertexID> jobVertexIds = new HashSet<>();

    public ForwardGroup(final Set<ExecutionJobVertex> jobVertices) {
        checkNotNull(jobVertices);

        Set<Integer> decidedParallelisms =
                jobVertices.stream()
                        .filter(
                                jobVertex -> {
                                    jobVertexIds.add(jobVertex.getJobVertexId());
                                    return jobVertex.isParallelismDecided();
                                })
                        .map(ExecutionJobVertex::getParallelism)
                        .collect(Collectors.toSet());

        checkState(decidedParallelisms.size() <= 1);
        if (decidedParallelisms.size() == 1) {
            this.parallelism = decidedParallelisms.iterator().next();
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

    public int size() {
        return jobVertexIds.size();
    }

    public Set<JobVertexID> getJobVertexIds() {
        return jobVertexIds;
    }
}
