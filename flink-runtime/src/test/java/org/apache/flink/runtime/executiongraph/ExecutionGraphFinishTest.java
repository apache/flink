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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests the finish behaviour of the {@link ExecutionGraph}. */
public class ExecutionGraphFinishTest extends TestLogger {

    @Test
    public void testJobFinishes() throws Exception {

        JobGraph jobGraph =
                JobGraphTestUtils.streamingJobGraph(
                        ExecutionGraphTestUtils.createJobVertex("Task1", 2, NoOpInvokable.class),
                        ExecutionGraphTestUtils.createJobVertex("Task2", 2, NoOpInvokable.class));

        SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .build();

        ExecutionGraph eg = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        Iterator<ExecutionJobVertex> jobVertices = eg.getVerticesTopologically().iterator();

        ExecutionJobVertex sender = jobVertices.next();
        ExecutionJobVertex receiver = jobVertices.next();

        List<ExecutionVertex> senderVertices = Arrays.asList(sender.getTaskVertices());
        List<ExecutionVertex> receiverVertices = Arrays.asList(receiver.getTaskVertices());

        // test getNumExecutionVertexFinished
        senderVertices.get(0).getCurrentExecutionAttempt().markFinished();
        assertEquals(1, sender.getNumExecutionVertexFinished());
        assertEquals(JobStatus.RUNNING, eg.getState());

        senderVertices.get(1).getCurrentExecutionAttempt().markFinished();
        assertEquals(2, sender.getNumExecutionVertexFinished());
        assertEquals(JobStatus.RUNNING, eg.getState());

        // test job finishes
        receiverVertices.get(0).getCurrentExecutionAttempt().markFinished();
        receiverVertices.get(1).getCurrentExecutionAttempt().markFinished();
        assertEquals(4, eg.getNumFinishedVertices());
        assertEquals(JobStatus.FINISHED, eg.getState());
    }
}
