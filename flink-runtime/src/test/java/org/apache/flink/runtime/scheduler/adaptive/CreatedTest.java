/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Created} state. */
public class CreatedTest extends TestLogger {

    @Test
    public void testCancel() throws Exception {
        try (MockCreatedContext ctx = new MockCreatedContext()) {
            Created created = new Created(ctx, log);

            ctx.setExpectFinished(assertNonNull());

            created.cancel();
        }
    }

    @Test
    public void testStartScheduling() throws Exception {
        try (MockCreatedContext ctx = new MockCreatedContext()) {
            Created created = new Created(ctx, log);

            ctx.setExpectWaitingForResources();

            created.startScheduling();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockCreatedContext ctx = new MockCreatedContext()) {
            Created created = new Created(ctx, log);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });

            created.suspend(new RuntimeException("Suspend"));
        }
    }

    @Test
    public void testFailure() throws Exception {
        try (MockCreatedContext ctx = new MockCreatedContext()) {
            Created created = new Created(ctx, log);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED));
                    });

            created.handleGlobalFailure(new RuntimeException("Global"));
        }
    }

    @Test
    public void testJobInformation() throws Exception {
        try (MockCreatedContext ctx = new MockCreatedContext()) {
            Created created = new Created(ctx, log);
            ArchivedExecutionGraph job = created.getJob();
            assertThat(job.getState(), is(JobStatus.INITIALIZING));
        }
    }

    static class MockCreatedContext implements Created.Context, AutoCloseable {
        private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("finished");
        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishedStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return ArchivedExecutionGraph.createFromInitializingJob(
                    new JobID(), "testJob", jobStatus, cause, 0L);
        }

        @Override
        public void goToWaitingForResources() {
            waitingForResourcesStateValidator.validateInput(null);
        }

        @Override
        public void close() throws Exception {
            finishedStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }
}
