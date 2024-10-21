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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobmaster.event.JobEvent;

import java.util.function.BiConsumer;

/**
 * The {@code AdaptiveExecutionHandler} interface defines the operations for handling the adaptive
 * execution of streaming jobs. This includes acquiring the current job graph and dynamically
 * adjusting the job topology in response to job events.
 */
public interface AdaptiveExecutionHandler {

    /**
     * Returns the {@code JobGraph} representing the batch job.
     *
     * @return the JobGraph.
     */
    JobGraph getJobGraph();

    /**
     * Handles the provided {@code JobEvent}, attempting dynamic modifications to the {@code
     * StreamGraph} based on the specifics of the job event.
     *
     * @param jobEvent The job event to handle. This event contains the necessary information that
     *     might trigger adjustments to the StreamGraph.
     */
    void handleJobEvent(JobEvent jobEvent);

    /**
     * Registers a listener to receive updates when the {@code JobGraph} is modified.
     *
     * @param listener the listener to register for JobGraph updates.
     */
    void registerJobGraphUpdateListener(JobGraphUpdateListener listener);

    /**
     * Retrieves the {@code ForwardGroup} for a given JobVertex ID.
     *
     * @param jobVertexId the ID of the JobVertex.
     * @return the corresponding ForwardGroup.
     */
    ForwardGroup getForwardGroupByJobVertexId(JobVertexID jobVertexId);

    /**
     * Updates the parallelism of the forward group associated with a specific JobVertex id. This
     * update also applies to the parallelism of all StreamNodes or JobVertices that belong to this
     * forward group.
     *
     * @param jobVertexId the ID of the JobVertex.
     * @param newParallelism the new parallelism level to set.
     * @param jobVertexParallelismUpdater a BiConsumer that updates the execution JobVertex
     *     parallelism.
     */
    void updateForwardGroupParallelism(
            JobVertexID jobVertexId,
            int newParallelism,
            BiConsumer<JobVertexID, Integer> jobVertexParallelismUpdater);

    /**
     * Creates an instance of {@code StreamGraphTopologyContext}.
     *
     * @param defaultMaxParallelism the default value for maximum parallelism.
     * @return an instance of {@code StreamGraphTopologyContext}.
     */
    StreamGraphTopologyContext createStreamGraphTopologyContext(int defaultMaxParallelism);
}
