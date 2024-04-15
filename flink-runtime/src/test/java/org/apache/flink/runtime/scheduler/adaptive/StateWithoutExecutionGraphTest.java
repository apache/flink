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
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StateWithoutExecutionGraph} state. */
public class StateWithoutExecutionGraphTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreatedTest.class);

    @RegisterExtension
    MockStateWithoutExecutionGraphContext ctx = new MockStateWithoutExecutionGraphContext();

    @Test
    void testCancelTransitionsToFinished() {
        TestingStateWithoutExecutionGraph state = new TestingStateWithoutExecutionGraph(ctx, LOG);

        ctx.setExpectFinished(
                archivedExecutionGraph -> {
                    assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.CANCELED);
                    assertThat(archivedExecutionGraph.getFailureInfo()).isNull();
                });
        state.cancel();
    }

    @Test
    void testSuspendTransitionsToFinished() {
        FlinkException expectedException = new FlinkException("This is a test exception");
        TestingStateWithoutExecutionGraph state = new TestingStateWithoutExecutionGraph(ctx, LOG);

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

        state.suspend(expectedException);
    }

    @Test
    void testTransitionToFinishedOnGlobalFailure() {
        TestingStateWithoutExecutionGraph state = new TestingStateWithoutExecutionGraph(ctx, LOG);
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

        state.handleGlobalFailure(expectedException, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
    }

    private static final class TestingStateWithoutExecutionGraph
            extends StateWithoutExecutionGraph {

        TestingStateWithoutExecutionGraph(Context context, Logger logger) {
            super(context, logger);
        }

        @Override
        public JobStatus getJobStatus() {
            return null;
        }
    }
}
