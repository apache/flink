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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AdaptiveScheduler AdaptiveScheduler's} {@link Finished} state. */
class FinishedTest {

    private static final Logger log = LoggerFactory.getLogger(FinishedTest.class);
    private final JobStatus testJobStatus = JobStatus.FAILED;

    @Test
    void testOnFinishedCallOnEnter() {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx);

        assertThat(ctx.getArchivedExecutionGraph().getState()).isEqualTo(testJobStatus);
    }

    @Test
    void testCancelIgnored() {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx).cancel();
        assertThat(ctx.getArchivedExecutionGraph().getState()).isEqualTo(testJobStatus);
    }

    @Test
    void testSuspendIgnored() {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx).suspend(new RuntimeException());
        assertThat(ctx.getArchivedExecutionGraph().getState()).isEqualTo(testJobStatus);
    }

    @Test
    void testGlobalFailureIgnored() {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx)
                .handleGlobalFailure(
                        new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        assertThat(ctx.getArchivedExecutionGraph().getState()).isEqualTo(testJobStatus);
    }

    @Test
    void testGetJobStatus() {
        MockFinishedContext ctx = new MockFinishedContext();
        assertThat(createFinishedState(ctx).getJobStatus()).isEqualTo(testJobStatus);
    }

    private Finished createFinishedState(MockFinishedContext ctx) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                new ArchivedExecutionGraphBuilder().setState(testJobStatus).build();
        return new Finished(ctx, archivedExecutionGraph, log);
    }

    private static class MockFinishedContext implements Finished.Context {

        private ArchivedExecutionGraph archivedExecutionGraph = null;

        @Override
        public void onFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            if (archivedExecutionGraph != null) {
                this.archivedExecutionGraph = archivedExecutionGraph;
            } else {
                throw new AssertionError("Transitioned to onFinished twice");
            }
        }

        private ArchivedExecutionGraph getArchivedExecutionGraph() {
            return archivedExecutionGraph;
        }
    }
}
