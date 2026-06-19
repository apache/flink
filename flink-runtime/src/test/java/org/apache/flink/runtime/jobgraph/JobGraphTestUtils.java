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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import java.util.Arrays;
import java.util.function.Function;

/** Utilities for creating {@link JobGraph JobGraphs} for testing purposes. */
public class JobGraphTestUtils {

    public static JobGraph emptyJobGraph() {
        final JobGraph jobGraph = JobGraphBuilder.newStreamingJobGraphBuilder().build();
        jobGraph.setApplicationId(new ApplicationID());
        return jobGraph;
    }

    public static JobGraph singleNoOpJobGraph() {
        JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(1);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder().addJobVertex(jobVertex).build();
        jobGraph.setApplicationId(new ApplicationID());
        return jobGraph;
    }

    public static JobGraph streamingJobGraph(JobVertex... jobVertices) {
        return streamingJobGraph(Function.identity(), jobVertices);
    }

    public static JobGraph streamingJobGraph(
            Function<JobGraphBuilder, JobGraphBuilder> transformJgb, JobVertex... jobVertices) {
        final JobGraph jobGraph =
                transformJgb
                        .apply(
                                JobGraphBuilder.newStreamingJobGraphBuilder()
                                        .addJobVertices(Arrays.asList(jobVertices)))
                        .build();
        jobGraph.setApplicationId(new ApplicationID());
        return jobGraph;
    }

    public static JobGraph batchJobGraph(JobVertex... jobVertices) {
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .addJobVertices(Arrays.asList(jobVertices))
                        .build();
        jobGraph.setApplicationId(new ApplicationID());
        return jobGraph;
    }

    private JobGraphTestUtils() {
        throw new UnsupportedOperationException("This class should never be instantiated.");
    }
}
