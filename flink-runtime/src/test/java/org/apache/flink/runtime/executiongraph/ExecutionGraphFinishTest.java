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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the finish behaviour of the {@link ExecutionGraph}. */
class ExecutionGraphFinishTest {

    /**
     * Dedicated single-threaded main-thread executor; using forMainThread() here would run the
     * deployment callbacks on the I/O thread and race the test (FLINK-38536).
     */
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> JM_MAIN_THREAD_EXECUTOR_RESOURCE =
            TestingUtils.jmMainThreadExecutorExtension();

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testJobFinishes() throws Exception {
        JobGraph jobGraph =
                JobGraphTestUtils.streamingJobGraph(
                        ExecutionGraphTestUtils.createJobVertex("Task1", 2, NoOpInvokable.class),
                        ExecutionGraphTestUtils.createJobVertex("Task2", 2, NoOpInvokable.class));

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        JM_MAIN_THREAD_EXECUTOR_RESOURCE.getExecutor());

        SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                        .build();

        ExecutionGraph eg = scheduler.getExecutionGraph();

        runInMainThread(mainThreadExecutor, scheduler::startScheduling);
        runInMainThread(
                mainThreadExecutor, () -> ExecutionGraphTestUtils.switchAllVerticesToRunning(eg));

        final ExecutionJobVertex[] orderedVertices =
                supplyInMainThread(
                        mainThreadExecutor,
                        () -> {
                            Iterator<ExecutionJobVertex> jobVertices =
                                    eg.getVerticesTopologically().iterator();
                            return new ExecutionJobVertex[] {
                                jobVertices.next(), jobVertices.next()
                            };
                        });
        ExecutionJobVertex sender = orderedVertices[0];
        ExecutionJobVertex receiver = orderedVertices[1];

        List<ExecutionVertex> senderVertices =
                supplyInMainThread(
                        mainThreadExecutor, () -> Arrays.asList(sender.getTaskVertices()));
        List<ExecutionVertex> receiverVertices =
                supplyInMainThread(
                        mainThreadExecutor, () -> Arrays.asList(receiver.getTaskVertices()));

        // test getNumExecutionVertexFinished
        runInMainThread(
                mainThreadExecutor,
                () -> senderVertices.get(0).getCurrentExecutionAttempt().markFinished());
        assertThat(supplyInMainThread(mainThreadExecutor, sender::getNumExecutionVertexFinished))
                .isOne();
        assertThat(supplyInMainThread(mainThreadExecutor, eg::getState))
                .isEqualTo(JobStatus.RUNNING);

        runInMainThread(
                mainThreadExecutor,
                () -> senderVertices.get(1).getCurrentExecutionAttempt().markFinished());
        assertThat(supplyInMainThread(mainThreadExecutor, sender::getNumExecutionVertexFinished))
                .isEqualTo(2);
        assertThat(supplyInMainThread(mainThreadExecutor, eg::getState))
                .isEqualTo(JobStatus.RUNNING);

        // test job finishes
        runInMainThread(
                mainThreadExecutor,
                () -> {
                    receiverVertices.get(0).getCurrentExecutionAttempt().markFinished();
                    receiverVertices.get(1).getCurrentExecutionAttempt().markFinished();
                });
        assertThat(eg.waitUntilTerminal()).isEqualTo(JobStatus.FINISHED);
        assertThat(supplyInMainThread(mainThreadExecutor, eg::getNumFinishedVertices)).isEqualTo(4);
        assertThat(supplyInMainThread(mainThreadExecutor, eg::getState))
                .isEqualTo(JobStatus.FINISHED);
    }

    /** Runs the action on the JobManager main thread and blocks until it completes. */
    private static void runInMainThread(
            final ComponentMainThreadExecutor mainThreadExecutor, final Runnable action)
            throws Exception {
        CompletableFuture.runAsync(action, mainThreadExecutor).get();
    }

    /** Reads the value on the JobManager main thread and blocks until it is available. */
    private static <T> T supplyInMainThread(
            final ComponentMainThreadExecutor mainThreadExecutor, final Supplier<T> supplier)
            throws Exception {
        return CompletableFuture.supplyAsync(supplier, mainThreadExecutor).get();
    }
}
