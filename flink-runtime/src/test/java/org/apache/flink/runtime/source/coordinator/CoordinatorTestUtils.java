/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A util class containing the helper methods for the coordinator tests. */
class CoordinatorTestUtils {

    /**
     * Create a SplitsAssignment. The assignments looks like following: Subtask 0: Splits {0}
     * Subtask 1: Splits {1, 2} Subtask 2: Splits {3, 4, 5}
     */
    static SplitsAssignment<MockSourceSplit> getSplitsAssignment(
            int numSubtasks, int startingSplitId) {
        Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
        int splitId = startingSplitId;
        for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
            List<MockSourceSplit> subtaskAssignment = new ArrayList<>();
            for (int j = 0; j < subtaskIndex + 1; j++) {
                subtaskAssignment.add(new MockSourceSplit(splitId++));
            }
            assignments.put(subtaskIndex, subtaskAssignment);
        }
        return new SplitsAssignment<>(assignments);
    }

    /** Check the actual assignment meets the expectation. */
    static void verifyAssignment(
            List<String> expectedSplitIds, Collection<MockSourceSplit> actualAssignment) {
        assertThat(actualAssignment.size()).isEqualTo(expectedSplitIds.size());
        int i = 0;
        for (MockSourceSplit split : actualAssignment) {
            assertThat(split.splitId()).isEqualTo(expectedSplitIds.get(i++));
        }
    }

    static void verifyException(
            ThrowingRunnable<Throwable> runnable, String failureMessage, String errorMessage) {
        assertThatThrownBy(runnable::run, failureMessage).hasStackTraceContaining(errorMessage);
    }

    static void waitForCoordinatorToProcessActions(SourceCoordinatorContext<?> context) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        context.runInCoordinatorThread(() -> future.complete(null));

        try {
            future.get();
        } catch (InterruptedException e) {
            throw new AssertionError("test interrupted");
        } catch (ExecutionException e) {
            ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
        }
    }
}
