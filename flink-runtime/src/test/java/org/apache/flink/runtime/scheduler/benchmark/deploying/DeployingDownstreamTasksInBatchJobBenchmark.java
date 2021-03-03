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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark.deploying;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;

/**
 * The benchmark of deploying downstream tasks in a BATCH job. The related method is {@link
 * Execution#deploy}.
 */
public class DeployingDownstreamTasksInBatchJobBenchmark extends DeployingTasksBenchmarkBase {

    private ExecutionVertex[] vertices;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        createAndSetupExecutionGraph(jobConfiguration);

        final JobVertex source = jobVertices.get(0);

        for (ExecutionVertex ev : executionGraph.getJobVertex(source.getID()).getTaskVertices()) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }

        final JobVertex sink = jobVertices.get(1);

        vertices = executionGraph.getJobVertex(sink.getID()).getTaskVertices();
    }

    public void deployDownstreamTasks() throws Exception {
        for (ExecutionVertex ev : vertices) {
            Execution execution = ev.getCurrentExecutionAttempt();
            execution.deploy();
        }
    }
}
