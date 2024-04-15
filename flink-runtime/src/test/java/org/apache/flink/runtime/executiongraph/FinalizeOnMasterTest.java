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
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertex.FinalizeOnMasterContext;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createScheduler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests that the {@link JobVertex#finalizeOnMaster(FinalizeOnMasterContext)} is called properly and
 * only when the execution graph reaches the successful final state.
 */
class FinalizeOnMasterTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testFinalizeIsCalledUponSuccess() throws Exception {
        final JobVertex vertex1 = spy(new JobVertex("test vertex 1"));
        vertex1.setInvokableClass(NoOpInvokable.class);
        vertex1.setParallelism(3);

        final JobVertex vertex2 = spy(new JobVertex("test vertex 2"));
        vertex2.setInvokableClass(NoOpInvokable.class);
        vertex2.setParallelism(2);

        final SchedulerBase scheduler =
                createScheduler(
                        JobGraphTestUtils.streamingJobGraph(vertex1, vertex2),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());
        scheduler.startScheduling();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);

        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        // move all vertices to finished state
        ExecutionGraphTestUtils.finishAllVertices(eg);
        assertThat(eg.waitUntilTerminal()).isEqualTo(JobStatus.FINISHED);

        verify(vertex1, times(1)).finalizeOnMaster(any(FinalizeOnMasterContext.class));
        verify(vertex2, times(1)).finalizeOnMaster(any(FinalizeOnMasterContext.class));

        assertThat(eg.getRegisteredExecutions()).isEmpty();
    }

    @Test
    void testFinalizeIsNotCalledUponFailure() throws Exception {
        final JobVertex vertex = spy(new JobVertex("test vertex 1"));
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(1);

        final SchedulerBase scheduler =
                createScheduler(
                        JobGraphTestUtils.streamingJobGraph(vertex),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());
        scheduler.startScheduling();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);

        ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

        // fail the execution
        final Execution exec =
                eg.getJobVertex(vertex.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();
        exec.fail(new Exception("test"));

        assertThat(eg.waitUntilTerminal()).isEqualTo(JobStatus.FAILED);

        verify(vertex, times(0)).finalizeOnMaster(any(FinalizeOnMasterContext.class));

        assertThat(eg.getRegisteredExecutions()).isEmpty();
    }
}
