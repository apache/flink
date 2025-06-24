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
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

class ArchivedExecutionGraphTestUtils {

    private ArchivedExecutionGraphTestUtils() {}

    static void compareExecutionVertex(
            AccessExecutionVertex runtimeVertex, AccessExecutionVertex archivedVertex) {
        assertThat(runtimeVertex.getTaskNameWithSubtaskIndex())
                .isEqualTo(archivedVertex.getTaskNameWithSubtaskIndex());
        assertThat(runtimeVertex.getParallelSubtaskIndex())
                .isEqualTo(archivedVertex.getParallelSubtaskIndex());
        assertThat(runtimeVertex.getExecutionState()).isEqualTo(archivedVertex.getExecutionState());
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.CREATED))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.CREATED));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.SCHEDULED))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.SCHEDULED));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.DEPLOYING))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.DEPLOYING));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.INITIALIZING))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.INITIALIZING));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.RUNNING))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.RUNNING));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.FINISHED))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.FINISHED));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.CANCELING))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.CANCELING));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.CANCELED))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.CANCELED));
        assertThat(runtimeVertex.getStateTimestamp(ExecutionState.FAILED))
                .isEqualTo(archivedVertex.getStateTimestamp(ExecutionState.FAILED));
        assertThat(runtimeVertex.getFailureInfo().map(ErrorInfo::getExceptionAsString))
                .isEqualTo(archivedVertex.getFailureInfo().map(ErrorInfo::getExceptionAsString));
        assertThat(runtimeVertex.getFailureInfo().map(ErrorInfo::getTimestamp))
                .isEqualTo(archivedVertex.getFailureInfo().map(ErrorInfo::getTimestamp));
        assertThat(runtimeVertex.getCurrentAssignedResourceLocation())
                .isEqualTo(archivedVertex.getCurrentAssignedResourceLocation());

        compareExecution(
                runtimeVertex.getCurrentExecutionAttempt(),
                archivedVertex.getCurrentExecutionAttempt());

        compareExecutions(
                runtimeVertex.getCurrentExecutions(), archivedVertex.getCurrentExecutions());
    }

    private static <RT extends AccessExecution, AT extends AccessExecution> void compareExecutions(
            Collection<RT> runtimeExecutions, Collection<AT> archivedExecutions) {
        assertThat(runtimeExecutions).hasSameSizeAs(archivedExecutions);

        List<RT> sortedRuntimeExecutions = new ArrayList<>(runtimeExecutions);
        List<AT> sortedArchivedExecutions = new ArrayList<>(archivedExecutions);
        sortedRuntimeExecutions.sort(Comparator.comparingInt(AccessExecution::getAttemptNumber));
        sortedArchivedExecutions.sort(Comparator.comparingInt(AccessExecution::getAttemptNumber));

        for (int i = 0; i < runtimeExecutions.size(); i++) {
            compareExecution(sortedRuntimeExecutions.get(i), sortedArchivedExecutions.get(i));
        }
    }

    private static void compareExecution(
            AccessExecution runtimeExecution, AccessExecution archivedExecution) {
        assertThat(runtimeExecution.getAttemptId()).isEqualTo(archivedExecution.getAttemptId());
        assertThat(runtimeExecution.getAttemptNumber())
                .isEqualTo(archivedExecution.getAttemptNumber());
        assertThat(runtimeExecution.getStateTimestamps())
                .containsExactly(archivedExecution.getStateTimestamps());
        assertThat(runtimeExecution.getStateEndTimestamps())
                .containsExactly(archivedExecution.getStateEndTimestamps());
        assertThat(runtimeExecution.getState()).isEqualTo(archivedExecution.getState());
        assertThat(runtimeExecution.getAssignedResourceLocation())
                .isEqualTo(archivedExecution.getAssignedResourceLocation());
        assertThat(runtimeExecution.getFailureInfo().map(ErrorInfo::getExceptionAsString))
                .isEqualTo(archivedExecution.getFailureInfo().map(ErrorInfo::getExceptionAsString));
        assertThat(runtimeExecution.getFailureInfo().map(ErrorInfo::getTimestamp))
                .isEqualTo(archivedExecution.getFailureInfo().map(ErrorInfo::getTimestamp));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.CREATED))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.CREATED));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.SCHEDULED))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.SCHEDULED));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.DEPLOYING))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.DEPLOYING));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.INITIALIZING))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.INITIALIZING));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.RUNNING))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.RUNNING));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.FINISHED))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.FINISHED));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.CANCELING))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.CANCELING));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.CANCELED))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.CANCELED));
        assertThat(runtimeExecution.getStateTimestamp(ExecutionState.FAILED))
                .isEqualTo(archivedExecution.getStateTimestamp(ExecutionState.FAILED));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.CREATED))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.CREATED));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.SCHEDULED))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.SCHEDULED));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.DEPLOYING))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.DEPLOYING));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.INITIALIZING))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.INITIALIZING));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.RUNNING))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.RUNNING));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.FINISHED))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.FINISHED));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.CANCELING))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.CANCELING));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.CANCELED))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.CANCELED));
        assertThat(runtimeExecution.getStateEndTimestamp(ExecutionState.FAILED))
                .isEqualTo(archivedExecution.getStateEndTimestamp(ExecutionState.FAILED));
        compareStringifiedAccumulators(
                runtimeExecution.getUserAccumulatorsStringified(),
                archivedExecution.getUserAccumulatorsStringified());
        assertThat(runtimeExecution.getParallelSubtaskIndex())
                .isEqualTo(archivedExecution.getParallelSubtaskIndex());
    }

    static void compareStringifiedAccumulators(
            StringifiedAccumulatorResult[] runtimeAccs,
            StringifiedAccumulatorResult[] archivedAccs) {
        assertThat(runtimeAccs.length).isEqualTo(archivedAccs.length);

        for (int x = 0; x < runtimeAccs.length; x++) {
            StringifiedAccumulatorResult runtimeResult = runtimeAccs[x];
            StringifiedAccumulatorResult archivedResult = archivedAccs[x];

            assertThat(runtimeResult.getName()).isEqualTo(archivedResult.getName());
            assertThat(runtimeResult.getType()).isEqualTo(archivedResult.getType());
            assertThat(runtimeResult.getValue()).isEqualTo(archivedResult.getValue());
        }
    }

    static void compareSerializedAccumulators(
            Map<String, SerializedValue<OptionalFailure<Object>>> runtimeAccs,
            Map<String, SerializedValue<OptionalFailure<Object>>> archivedAccs)
            throws IOException, ClassNotFoundException {
        assertThat(runtimeAccs).hasSameSizeAs(archivedAccs);
        for (Entry<String, SerializedValue<OptionalFailure<Object>>> runtimeAcc :
                runtimeAccs.entrySet()) {
            long runtimeUserAcc =
                    (long)
                            runtimeAcc
                                    .getValue()
                                    .deserializeValue(ClassLoader.getSystemClassLoader())
                                    .getUnchecked();
            long archivedUserAcc =
                    (long)
                            archivedAccs
                                    .get(runtimeAcc.getKey())
                                    .deserializeValue(ClassLoader.getSystemClassLoader())
                                    .getUnchecked();

            assertThat(runtimeUserAcc).isEqualTo(archivedUserAcc);
        }
    }
}
