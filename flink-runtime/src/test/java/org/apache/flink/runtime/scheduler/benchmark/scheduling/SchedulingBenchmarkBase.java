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

package org.apache.flink.runtime.scheduler.benchmark.scheduling;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkBase;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulerOperations;

import java.util.List;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createAndInitExecutionGraph;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;

/** The base class of benchmarks related to scheduling tasks. */
public class SchedulingBenchmarkBase extends SchedulerBenchmarkBase {

    TestingSchedulerOperations schedulerOperations;
    List<JobVertex> jobVertices;
    ExecutionGraph executionGraph;
    SchedulingTopology schedulingTopology;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        super.setup();

        schedulerOperations = new TestingSchedulerOperations();
        jobVertices = createDefaultJobVertices(jobConfiguration);
        executionGraph =
                createAndInitExecutionGraph(
                        jobVertices, jobConfiguration, scheduledExecutorService);
        schedulingTopology = executionGraph.getSchedulingTopology();
    }
}
