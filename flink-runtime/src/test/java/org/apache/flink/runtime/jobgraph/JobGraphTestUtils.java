/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.testtasks.NoOpInvokable;

/** Utilities for creating {@link JobGraph JobGraphs} for testing purposes. */
public class JobGraphTestUtils {

    public static JobGraph emptyJobGraph() {
        final JobGraph jobGraph = new JobGraph();
        jobGraph.setJobType(JobType.STREAMING);

        return jobGraph;
    }

    public static JobGraph singleNoOpJobGraph() {
        JobGraph jobGraph = new JobGraph();
        JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(jobVertex);
        jobGraph.setJobType(JobType.STREAMING);

        return jobGraph;
    }

    public static JobGraph singleVertexJobGraph(JobVertex jobVertex) {
        JobGraph jobGraph = new JobGraph();
        jobGraph.addVertex(jobVertex);
        jobGraph.setJobType(JobType.STREAMING);

        return jobGraph;
    }

    private JobGraphTestUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
