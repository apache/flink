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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Created} state. */
class CreatedTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreatedTest.class);

    @RegisterExtension MockCreatedContext ctx = new MockCreatedContext();

    @Test
    void testCancel() {
        Created created = new Created(ctx, LOG);

        ctx.setExpectFinished(
                archivedExecutionGraph -> {
                    assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.CANCELED);
                    assertThat(archivedExecutionGraph.getFailureInfo()).isNull();
                });
        created.cancel();
    }

    @Test
    void testStartScheduling() {
        Created created = new Created(ctx, LOG);

        ctx.setExpectWaitingForResources();

        created.startScheduling();
    }

    @Test
    void testSuspend() {
        FlinkException expectedException = new FlinkException("This is a test exception");
        Created created = new Created(ctx, LOG);

        ctx.setExpectFinished(
                archivedExecutionGraph -> {
                    assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.SUSPENDED);
                    assertThat(archivedExecutionGraph.getFailureInfo()).isNotNull();
                    assertThat(
                                    archivedExecutionGraph
                                            .getFailureInfo()
                                            .getException()
                                            .deserializeError(this.getClass().getClassLoader()))
                            .isEqualTo(expectedException);
                });

        created.suspend(expectedException);
    }

    @Test
    void testFailure() {
        Created created = new Created(ctx, LOG);
        RuntimeException expectedException = new RuntimeException("This is a test exception");

        ctx.setExpectFinished(
                archivedExecutionGraph -> {
                    assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.FAILED);
                    assertThat(archivedExecutionGraph.getFailureInfo()).isNotNull();
                    assertThat(
                                    archivedExecutionGraph
                                            .getFailureInfo()
                                            .getException()
                                            .deserializeError(this.getClass().getClassLoader()))
                            .isEqualTo(expectedException);
                });

        created.handleGlobalFailure(expectedException, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
    }

    @Test
    void testJobInformation() {
        Created created = new Created(ctx, LOG);
        ArchivedExecutionGraph job = created.getJob();
        assertThat(job.getState()).isEqualTo(JobStatus.INITIALIZING);
    }

    static class MockCreatedContext implements Created.Context, AfterEachCallback {
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
            return ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                    new JobID(), "testJob", jobStatus, cause, null, 0L);
        }

        @Override
        public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
            waitingForResourcesStateValidator.validateInput(null);
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            finishedStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }
}
