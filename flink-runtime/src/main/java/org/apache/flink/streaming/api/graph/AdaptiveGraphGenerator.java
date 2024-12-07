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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

/**
 * Defines the mechanism for dynamically adapting the graph topology of a Flink job at runtime. The
 * AdaptiveGraphGenerator is responsible for managing and updating the job's execution plan based on
 * runtime events such as the completion of a job vertex. It provides functionalities to generate
 * new job vertices, retrieve the current JobGraph, update the StreamGraph, and track the status of
 * pending stream nodes.
 */
@Internal
public interface AdaptiveGraphGenerator {

    /**
     * Retrieves the JobGraph representation of the current state of the Flink job.
     *
     * @return The {@link JobGraph} instance.
     */
    JobGraph getJobGraph();

    /**
     * Retrieves the StreamGraphContext which provides a read-only view of the StreamGraph and
     * methods to modify its StreamEdges and StreamNodes.
     *
     * @return an instance of {@link StreamGraphContext}.
     */
    StreamGraphContext getStreamGraphContext();

    /**
     * Responds to notifications that a JobVertex has completed execution. This method generates new
     * job vertices, incorporates them into the JobGraph, and returns a list of the newly created
     * JobVertex instances.
     *
     * @param finishedJobVertexId The ID of the completed JobVertex.
     * @return A list of the newly added {@link JobVertex} instances to the JobGraph.
     */
    List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId);
}
