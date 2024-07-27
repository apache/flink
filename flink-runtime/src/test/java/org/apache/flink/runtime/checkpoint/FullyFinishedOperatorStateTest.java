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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateSampleOperatorSubtaskState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the properties of {@link FullyFinishedOperatorState}. */
class FullyFinishedOperatorStateTest {

    @Test
    void testFullyFinishedOperatorState() {
        OperatorState operatorState = new FullyFinishedOperatorState(new OperatorID(), 5, 256);
        assertThat(operatorState.isFullyFinished()).isTrue();

        assertThat(operatorState.getSubtaskStates()).isEmpty();
        assertThat(operatorState.getStates()).isEmpty();
        assertThat(operatorState.getNumberCollectedStates()).isZero();

        assertThatThrownBy(() -> operatorState.putState(0, OperatorSubtaskState.builder().build()))
                .as("Should not be able to put new subtask states for a fully finished state")
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(
                        () ->
                                operatorState.setCoordinatorState(
                                        new ByteStreamStateHandle("test", new byte[] {1, 2, 3, 4})))
                .as("Should not be able to put new subtask states for a fully finished state")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testGetDiscardables() throws IOException {
        Tuple2<List<StateObject>, OperatorSubtaskState> opSubtaskStates1 =
                generateSampleOperatorSubtaskState();
        Tuple2<List<StateObject>, OperatorSubtaskState> opSubtaskStates2 =
                generateSampleOperatorSubtaskState();

        OperatorState operatorState = new OperatorState(new OperatorID(), 2, 256);
        operatorState.putState(0, opSubtaskStates1.f1);
        operatorState.putState(1, opSubtaskStates2.f1);
        ByteStreamStateHandle coordinatorState =
                new ByteStreamStateHandle("test", new byte[] {1, 2, 3, 4});
        operatorState.setCoordinatorState(coordinatorState);
        HashSet<StateObject> discardables = new HashSet<>();
        discardables.addAll(opSubtaskStates1.f0.subList(0, 4));
        discardables.add(((InputChannelStateHandle) opSubtaskStates1.f0.get(4)).getDelegate());
        discardables.add(
                ((ResultSubpartitionStateHandle) opSubtaskStates1.f0.get(5)).getDelegate());
        discardables.addAll(opSubtaskStates2.f0.subList(0, 4));
        discardables.add(((InputChannelStateHandle) opSubtaskStates2.f0.get(4)).getDelegate());
        discardables.add(
                ((ResultSubpartitionStateHandle) opSubtaskStates2.f0.get(5)).getDelegate());
        discardables.add(coordinatorState);
        assertThat(new HashSet<>(operatorState.getDiscardables())).isEqualTo(discardables);
    }
}
