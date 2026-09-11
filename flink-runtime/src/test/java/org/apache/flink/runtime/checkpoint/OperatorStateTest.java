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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the coordinator-state handling of {@link OperatorState}. */
class OperatorStateTest {

    private static ByteStreamStateHandle handle(String name) {
        return new ByteStreamStateHandle(name, new byte[] {1, 2, 3, 4});
    }

    @Test
    void testSetCoordinatorStateRejectsOverwrite() {
        OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 2, 256);
        ByteStreamStateHandle first = handle("first");
        operatorState.setCoordinatorState(first);

        assertThat(operatorState.getCoordinatorState()).isSameAs(first);
        assertThatThrownBy(() -> operatorState.setCoordinatorState(handle("second")))
                .as("setCoordinatorState must reject overwriting an already-set value")
                .isInstanceOf(IllegalStateException.class);
        assertThat(operatorState.getCoordinatorState()).isSameAs(first);
    }

    @Test
    void testOverwriteCoordinatorStateReplacesExistingValue() {
        OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 2, 256);
        operatorState.setCoordinatorState(handle("from-failed-attempt"));

        // Regional checkpoint fallback replaces the coordinator state collected during the failed
        // attempt with the historical one. This must not throw, unlike setCoordinatorState.
        ByteStreamStateHandle fallback = handle("from-historical");
        operatorState.overwriteCoordinatorState(fallback);

        assertThat(operatorState.getCoordinatorState()).isSameAs(fallback);
    }

    @Test
    void testOverwriteCoordinatorStateOnEmptyState() {
        OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 2, 256);
        assertThat(operatorState.getCoordinatorState()).isNull();

        ByteStreamStateHandle fallback = handle("from-historical");
        operatorState.overwriteCoordinatorState(fallback);

        assertThat(operatorState.getCoordinatorState()).isSameAs(fallback);
    }
}
