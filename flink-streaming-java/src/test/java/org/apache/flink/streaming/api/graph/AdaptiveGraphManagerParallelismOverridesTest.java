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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ParallelismOverridesGeneratorTestBase} for the incremental {@link AdaptiveGraphManager},
 * which creates job vertices chain by chain as upstream vertices finish.
 */
class AdaptiveGraphManagerParallelismOverridesTest extends ParallelismOverridesGeneratorTestBase {

    @Override
    JobGraph createJobGraph(StreamGraph streamGraph) {
        return generateJobGraphInLazilyMode(streamGraph);
    }

    static JobGraph generateJobGraphInLazilyMode(StreamGraph streamGraph) {
        final AdaptiveGraphManager adaptiveGraphManager =
                new AdaptiveGraphManager(
                        Thread.currentThread().getContextClassLoader(), streamGraph, Runnable::run);
        final JobGraph jobGraph = adaptiveGraphManager.getJobGraph();
        List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        while (!jobVertices.isEmpty()) {
            final List<JobVertex> created = new ArrayList<>();
            for (JobVertex jobVertex : jobVertices) {
                created.addAll(adaptiveGraphManager.onJobVertexFinished(jobVertex.getID()));
            }
            jobVertices = created;
        }
        return jobGraph;
    }
}
