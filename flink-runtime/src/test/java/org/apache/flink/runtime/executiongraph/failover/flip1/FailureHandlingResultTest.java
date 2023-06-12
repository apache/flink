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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionGraph;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailureHandlingResult}. */
class FailureHandlingResultTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /** Tests normal FailureHandlingResult. */
    @Test
    void testNormalFailureHandlingResult() throws Exception {

        // create a normal FailureHandlingResult
        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());

        Set<ExecutionVertexID> tasks = new HashSet<>();
        tasks.add(execution.getVertex().getID());

        final long delay = 1234;
        final Throwable error = new RuntimeException();
        final long timestamp = System.currentTimeMillis();
        final CompletableFuture<Map<String, String>> failureLabels =
                CompletableFuture.completedFuture(Collections.singletonMap("key", "value"));
        FailureHandlingResult result =
                FailureHandlingResult.restartable(
                        execution, error, timestamp, failureLabels, tasks, delay, false);

        assertThat(result.canRestart()).isTrue();
        assertThat(delay).isEqualTo(result.getRestartDelayMS());
        assertThat(tasks).isEqualTo(result.getVerticesToRestart());
        assertThat(result.getFailureLabels()).isEqualTo(failureLabels);
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
    }

    /** Tests FailureHandlingResult which suppresses restarts. */
    @Test
    void testRestartingSuppressedFailureHandlingResultWithNoCausingExecutionVertexId() {
        // create a FailureHandlingResult with error
        final Throwable error = new Exception("test error");
        final long timestamp = System.currentTimeMillis();
        final CompletableFuture<Map<String, String>> failureLabels =
                CompletableFuture.completedFuture(Collections.singletonMap("key", "value"));
        FailureHandlingResult result =
                FailureHandlingResult.unrecoverable(null, error, timestamp, failureLabels, false);

        assertThat(result.canRestart()).isFalse();
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getFailureLabels()).isEqualTo(failureLabels);
        assertThat(result.getFailedExecution()).isNotPresent();

        assertThatThrownBy(result::getVerticesToRestart)
                .as("getVerticesToRestart is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(result::getRestartDelayMS)
                .as("getRestartDelayMS is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);
    }

    static Execution createExecution(ScheduledExecutorService executor) throws Exception {
        final ExecutionGraph executionGraph = createExecutionGraph(executor, createNoOpVertex(1));
        return executionGraph.getRegisteredExecutions().values().iterator().next();
    }
}
