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

package org.apache.flink.runtime.scheduler.benchmark.topology;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkBase;

import java.util.List;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;

/**
 * The benchmark of building the topology of {@link ExecutionGraph} in a STREAMING/BATCH job. The
 * related method is {@link ExecutionGraph#attachJobGraph},
 */
public class BuildExecutionGraphBenchmark extends SchedulerBenchmarkBase {

    private List<JobVertex> jobVertices;
    private ExecutionGraph executionGraph;

    public BuildExecutionGraphBenchmark() {}

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        super.setup();

        jobVertices = createDefaultJobVertices(jobConfiguration);
        final JobGraph jobGraph = createJobGraph(jobConfiguration);
        final VertexParallelismStore parallelismStore =
                SchedulerBase.computeVertexParallelismStore(jobVertices);
        executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(parallelismStore)
                        .setJobGraph(jobGraph)
                        .build(scheduledExecutorService);
    }

    public void buildTopology() throws Exception {
        executionGraph.attachJobGraph(
                jobVertices, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
    }
}
