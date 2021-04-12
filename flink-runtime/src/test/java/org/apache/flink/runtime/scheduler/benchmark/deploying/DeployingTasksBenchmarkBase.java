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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;

import java.util.List;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createAndInitExecutionGraph;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;

/** The base class of benchmarks related to deploying tasks. */
public class DeployingTasksBenchmarkBase {

    List<JobVertex> jobVertices;
    ExecutionGraph executionGraph;

    public void createAndSetupExecutionGraph(JobConfiguration jobConfiguration) throws Exception {

        jobVertices = createDefaultJobVertices(jobConfiguration);

        executionGraph = createAndInitExecutionGraph(jobVertices, jobConfiguration);

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        for (ExecutionJobVertex ejv : executionGraph.getVerticesTopologically()) {
            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                final LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
                final Execution execution = ev.getCurrentExecutionAttempt();
                execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();
                if (!execution.tryAssignResource(slot)) {
                    throw new RuntimeException("Error when assigning slot to execution.");
                }
            }
        }
    }
}
