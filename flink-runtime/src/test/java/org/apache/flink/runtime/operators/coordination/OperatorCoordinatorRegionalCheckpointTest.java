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

package org.apache.flink.runtime.operators.coordination;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OperatorCoordinatorRegionalCheckpointTest {

    @Test
    void testDefaultSupportsRegionCheckpointReturnsFalse() {
        OperatorCoordinator coordinator = new TestingOperatorCoordinator();
        assertThat(coordinator.supportsRegionCheckpoint()).isFalse();
    }

    @Test
    void testDefaultCheckpointCoordinatorForRegionFallbackThrows() {
        OperatorCoordinator coordinator = new TestingOperatorCoordinator();
        Set<Integer> fallbackSubtaskIds = new HashSet<>();
        fallbackSubtaskIds.add(0);
        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();

        assertThatThrownBy(
                        () ->
                                coordinator.checkpointCoordinatorForRegionFallback(
                                        100L, 99L, fallbackSubtaskIds, resultFuture))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // Minimal OperatorCoordinator implementation for testing defaults.
    private static class TestingOperatorCoordinator implements OperatorCoordinator {
        @Override
        public void start() {}

        @Override
        public void close() {}

        @Override
        public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {}

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {}

        @Override
        public void subtaskReset(int subtask, long checkpointId) {}

        @Override
        public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {}

        @Override
        public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}
    }
}
