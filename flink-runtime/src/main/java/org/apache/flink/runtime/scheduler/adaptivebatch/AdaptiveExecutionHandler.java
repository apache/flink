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
import org.apache.flink.runtime.jobmaster.event.JobEvent;
import org.apache.flink.streaming.api.graph.StreamGraph;

/**
 * The {@link AdaptiveExecutionHandler} interface defines the operations for handling the adaptive
 * execution of batch jobs. This includes acquiring the current job graph and dynamically adjusting
 * the job topology in response to job events.
 */
public interface AdaptiveExecutionHandler {

    /**
     * Returns the {@link JobGraph} representing the batch job.
     *
     * @return the JobGraph.
     */
    JobGraph getJobGraph();

    /**
     * Handles the provided {@link JobEvent}, attempting dynamic modifications to the {@link
     * StreamGraph} based on the specifics of the job event.
     *
     * @param jobEvent The job event to handle. This event contains the necessary information that
     *     might trigger adjustments to the StreamGraph.
     */
    void handleJobEvent(JobEvent jobEvent);

    /**
     * Registers a listener to receive updates when the {@link JobGraph} is modified.
     *
     * @param listener the listener to register for JobGraph updates.
     */
    void registerJobGraphUpdateListener(JobGraphUpdateListener listener);

    /**
     * Retrieves the initial parallelism for a given JobVertex ID.
     *
     * @param jobVertexId the JobVertex ID.
     * @return the corresponding initial parallelism.
     */
    int getInitialParallelism(JobVertexID jobVertexId);

    /**
     * Notifies the handler that the parallelism of a JobVertex has been decided.
     *
     * @param jobVertexId the identifier of the JobVertex whose parallelism was decided.
     * @param parallelism the decided parallelism of the JobVertex.
     */
    void notifyJobVertexParallelismDecided(JobVertexID jobVertexId, int parallelism);

    /**
     * Creates an instance of {@link ExecutionPlanSchedulingContext}.
     *
     * @param defaultMaxParallelism the default value for maximum parallelism.
     * @return an instance of {@link ExecutionPlanSchedulingContext}.
     */
    ExecutionPlanSchedulingContext createExecutionPlanSchedulingContext(int defaultMaxParallelism);
}
