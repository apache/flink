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

package org.apache.flink.runtime.scheduler.benchmark.failover;

import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.switchAllVerticesToRunning;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.deployAllTasks;

/**
 * The benchmark of calculating region to restart when failover occurs in a STREAMING job. The
 * related method is {@link RestartPipelinedRegionFailoverStrategy#getTasksNeedingRestart}.
 */
public class RegionToRestartInStreamingJobBenchmark extends FailoverBenchmarkBase {

    @Override
    public void setup(JobConfiguration jobConfiguration) throws Exception {
        super.setup(jobConfiguration);

        TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployAllTasks(executionGraph, slotBuilder);

        switchAllVerticesToRunning(executionGraph);
    }

    public Set<ExecutionVertexID> calculateRegionToRestart() {
        return strategy.getTasksNeedingRestart(
                executionGraph.getJobVertex(source.getID()).getTaskVertices()[0].getID(),
                new Exception("For test."));
    }
}
