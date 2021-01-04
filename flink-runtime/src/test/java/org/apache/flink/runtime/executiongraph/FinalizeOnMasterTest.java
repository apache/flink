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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createScheduler;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests that the {@link JobVertex#finalizeOnMaster(ClassLoader)} is called properly and only when
 * the execution graph reaches the a successful final state.
 */
public class FinalizeOnMasterTest extends TestLogger {

    @Test
    public void testFinalizeIsCalledUponSuccess() throws Exception {
        final JobVertex vertex1 = spy(new JobVertex("test vertex 1"));
        vertex1.setInvokableClass(NoOpInvokable.class);
        vertex1.setParallelism(3);

        final JobVertex vertex2 = spy(new JobVertex("test vertex 2"));
        vertex2.setInvokableClass(NoOpInvokable.class);
        vertex2.setParallelism(2);

        final SchedulerBase scheduler = createScheduler(new JobGraph("Test Job", vertex1, vertex2));
        scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        scheduler.startScheduling();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertEquals(JobStatus.RUNNING, eg.getState());

        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        // move all vertices to finished state
        ExecutionGraphTestUtils.finishAllVertices(eg);
        assertEquals(JobStatus.FINISHED, eg.waitUntilTerminal());

        verify(vertex1, times(1)).finalizeOnMaster(any(ClassLoader.class));
        verify(vertex2, times(1)).finalizeOnMaster(any(ClassLoader.class));

        assertEquals(0, eg.getRegisteredExecutions().size());
    }

    @Test
    public void testFinalizeIsNotCalledUponFailure() throws Exception {
        final JobVertex vertex = spy(new JobVertex("test vertex 1"));
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(1);

        final SchedulerBase scheduler = createScheduler(new JobGraph("Test Job", vertex));
        scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        scheduler.startScheduling();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertEquals(JobStatus.RUNNING, eg.getState());

        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        // fail the execution
        final Execution exec =
                eg.getJobVertex(vertex.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();
        exec.fail(new Exception("test"));

        assertEquals(JobStatus.FAILED, eg.waitUntilTerminal());

        verify(vertex, times(0)).finalizeOnMaster(any(ClassLoader.class));

        assertEquals(0, eg.getRegisteredExecutions().size());
    }
}
