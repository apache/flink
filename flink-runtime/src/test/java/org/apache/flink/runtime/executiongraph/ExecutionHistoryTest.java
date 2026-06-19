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

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionHistory}. */
class ExecutionHistoryTest {

    @Test
    void testSizeLimit() {
        final ExecutionHistory history = new ExecutionHistory(2);

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        history.add(createArchivedExecution(createExecutionAttemptId(executionVertexId, 0)));
        history.add(createArchivedExecution(createExecutionAttemptId(executionVertexId, 1)));
        history.add(createArchivedExecution(createExecutionAttemptId(executionVertexId, 2)));

        assertThat(history.getHistoricalExecutions()).hasSize(2);
        assertThat(history.getHistoricalExecution(0)).isNotPresent();
        assertThat(history.getHistoricalExecution(1)).isPresent();
        assertThat(history.getHistoricalExecution(2)).isPresent();
    }

    @Test
    void testValidateAttemptNumber() {
        final ExecutionHistory history = new ExecutionHistory(2);

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        history.add(createArchivedExecution(createExecutionAttemptId(executionVertexId, 3)));

        assertThat(history.isValidAttemptNumber(2)).isTrue();
        assertThat(history.isValidAttemptNumber(3)).isTrue();
        assertThat(history.isValidAttemptNumber(4)).isFalse();
    }

    @Test
    void testGetWithInvalidAttemptNumber() {
        final ExecutionHistory history = new ExecutionHistory(2);

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        history.add(createArchivedExecution(createExecutionAttemptId(executionVertexId, 3)));

        assertThatThrownBy(() -> history.getHistoricalExecution(4))
                .as("IllegalArgumentException should happen")
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ArchivedExecution createArchivedExecution(ExecutionAttemptID attemptId) {
        return new ArchivedExecution(
                new StringifiedAccumulatorResult[0],
                null,
                attemptId,
                ExecutionState.CANCELED,
                null,
                null,
                null,
                new long[ExecutionState.values().length],
                new long[ExecutionState.values().length]);
    }
}
