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

import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.List;

/**
 * This interface defines operations for components that are interested in being notified when new
 * job vertices are added to the job graph.
 */
@FunctionalInterface
public interface JobGraphUpdateListener {
    /**
     * Invoked when new {@link JobVertex} instances are added to the JobGraph of a specific job.
     * This allows interested components to react to the addition of new vertices to the job
     * topology.
     *
     * @param newVertices A list of newly added JobVertex instances.
     * @param pendingOperatorsCount The number of pending operators.
     */
    void onNewJobVerticesAdded(List<JobVertex> newVertices, int pendingOperatorsCount)
            throws Exception;
}
