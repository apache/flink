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

package org.apache.flink.runtime.scheduler.benchmark.partitionrelease;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.RegionPartitionGroupReleaseStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkBase;

import java.util.List;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createAndInitExecutionGraph;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.deployTasks;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.transitionTaskStatus;

/**
 * The benchmark of releasing partitions in a BATCH job. The related method is {@link
 * RegionPartitionGroupReleaseStrategy#vertexFinished}.
 */
public class PartitionReleaseInBatchJobBenchmark extends SchedulerBenchmarkBase {

    private ExecutionGraph executionGraph;
    private JobVertex sink;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        super.setup();

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);

        executionGraph =
                createAndInitExecutionGraph(
                        jobVertices, jobConfiguration, scheduledExecutorService);

        final JobVertex source = jobVertices.get(0);
        sink = jobVertices.get(1);

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployTasks(executionGraph, source.getID(), slotBuilder);

        transitionTaskStatus(executionGraph, source.getID(), ExecutionState.FINISHED);

        deployTasks(executionGraph, sink.getID(), slotBuilder);
    }

    public void partitionRelease() {
        transitionTaskStatus(executionGraph, sink.getID(), ExecutionState.FINISHED);
    }
}
