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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The benchmark of scheduling downstream task in a BATCH job. The related method is {@link
 * PipelinedRegionSchedulingStrategy#onExecutionStateChange}.
 */
public class SchedulingDownstreamTasksInBatchJobBenchmark extends SchedulingBenchmarkBase {

    private ExecutionVertexID executionVertexID;
    private PipelinedRegionSchedulingStrategy schedulingStrategy;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        initSchedulingTopology(jobConfiguration);

        schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);

        // When we trying to scheduling downstream tasks via
        // onExecutionStateChange(ExecutionState.FINISHED),
        // the result partitions of upstream tasks need to be CONSUMABLE.
        // The CONSUMABLE status is determined by the variable "numberOfRunningProducers" of the
        // IntermediateResult.
        // Its value cannot be changed by any public methods.
        // So here we use reflections to modify this value and then schedule the downstream tasks.
        for (IntermediateResult result : executionGraph.getAllIntermediateResults().values()) {
            Field f = result.getClass().getDeclaredField("numberOfRunningProducers");
            f.setAccessible(true);
            AtomicInteger numberOfRunningProducers = (AtomicInteger) f.get(result);
            numberOfRunningProducers.set(0);
        }

        executionVertexID =
                executionGraph
                        .getJobVertex(jobVertices.get(0).getID())
                        .getTaskVertices()[0]
                        .getID();
    }

    public void schedulingDownstreamTasks() {
        schedulingStrategy.onExecutionStateChange(executionVertexID, ExecutionState.FINISHED);
    }
}
