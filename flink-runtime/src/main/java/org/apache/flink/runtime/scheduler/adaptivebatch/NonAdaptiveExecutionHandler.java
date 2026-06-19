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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.JobVertexForwardGroup;
import org.apache.flink.runtime.jobmaster.event.JobEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link NonAdaptiveExecutionHandler} implements the {@link AdaptiveExecutionHandler} interface
 * to provide an immutable static job graph.
 */
public class NonAdaptiveExecutionHandler implements AdaptiveExecutionHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final JobGraph jobGraph;
    private final Map<JobVertexID, JobVertexForwardGroup> forwardGroupsByJobVertexId;

    public NonAdaptiveExecutionHandler(JobGraph jobGraph) {
        this.jobGraph = checkNotNull(jobGraph);
        this.forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroupsAndCheckParallelism(
                        getJobGraph().getVerticesSortedTopologicallyFromSources());
    }

    @Override
    public JobGraph getJobGraph() {
        return jobGraph;
    }

    @Override
    public void handleJobEvent(JobEvent jobEvent) {
        // do nothing
    }

    @Override
    public void registerJobGraphUpdateListener(JobGraphUpdateListener listener) {
        // do nothing
    }

    @Override
    public int getInitialParallelism(JobVertexID jobVertexId) {
        JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);
        int vertexInitialParallelism = jobVertex.getParallelism();
        JobVertexForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertexId);

        if (jobVertex.getParallelism() == ExecutionConfig.PARALLELISM_DEFAULT
                && forwardGroup != null
                && forwardGroup.isParallelismDecided()) {
            vertexInitialParallelism = forwardGroup.getParallelism();
            log.info(
                    "Parallelism of JobVertex: {} ({}) is decided to be {} according to forward group's parallelism.",
                    jobVertex.getName(),
                    jobVertex,
                    vertexInitialParallelism);
        }

        return vertexInitialParallelism;
    }

    @Override
    public void notifyJobVertexParallelismDecided(JobVertexID jobVertexId, int parallelism) {
        JobVertexForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertexId);
        if (forwardGroup != null && !forwardGroup.isParallelismDecided()) {
            forwardGroup.setParallelism(parallelism);
        } else if (forwardGroup != null) {
            checkArgument(
                    forwardGroup.getParallelism() == parallelism,
                    "Incompatible parallelism for forward group.");
        }
    }

    @Override
    public ExecutionPlanSchedulingContext createExecutionPlanSchedulingContext(
            int defaultMaxParallelism) {
        return NonAdaptiveExecutionPlanSchedulingContext.INSTANCE;
    }
}
