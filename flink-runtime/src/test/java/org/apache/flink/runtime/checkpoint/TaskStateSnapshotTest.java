/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TaskStateSnapshotTest {

    @Test
    void putGetSubtaskStateByOperatorID() {
        TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();

        OperatorID operatorID_1 = new OperatorID();
        OperatorID operatorID_2 = new OperatorID();
        OperatorSubtaskState operatorSubtaskState_1 = OperatorSubtaskState.builder().build();
        OperatorSubtaskState operatorSubtaskState_2 = OperatorSubtaskState.builder().build();
        OperatorSubtaskState operatorSubtaskState_1_replace =
                OperatorSubtaskState.builder().build();

        assertThat(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1)).isNull();
        assertThat(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_2)).isNull();
        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);
        assertThat(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1))
                .isEqualTo(operatorSubtaskState_1);
        assertThat(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_2))
                .isEqualTo(operatorSubtaskState_2);
        assertThat(
                        taskStateSnapshot.putSubtaskStateByOperatorID(
                                operatorID_1, operatorSubtaskState_1_replace))
                .isEqualTo(operatorSubtaskState_1);
        assertThat(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1))
                .isEqualTo(operatorSubtaskState_1_replace);
    }

    @Test
    void hasState() {
        Random random = new Random(0x42);
        TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
        assertThat(taskStateSnapshot.hasState()).isFalse();

        OperatorSubtaskState emptyOperatorSubtaskState = OperatorSubtaskState.builder().build();
        assertThat(emptyOperatorSubtaskState.hasState()).isFalse();
        taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), emptyOperatorSubtaskState);
        assertThat(taskStateSnapshot.hasState()).isFalse();

        OperatorStateHandle stateHandle =
                StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
        OperatorSubtaskState nonEmptyOperatorSubtaskState =
                OperatorSubtaskState.builder().setManagedOperatorState(stateHandle).build();

        assertThat(nonEmptyOperatorSubtaskState.hasState()).isTrue();
        taskStateSnapshot.putSubtaskStateByOperatorID(
                new OperatorID(), nonEmptyOperatorSubtaskState);
        assertThat(taskStateSnapshot.hasState()).isTrue();
    }

    @Test
    void discardState() throws Exception {
        TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
        OperatorID operatorID_1 = new OperatorID();
        OperatorID operatorID_2 = new OperatorID();

        OperatorSubtaskState operatorSubtaskState_1 = mock(OperatorSubtaskState.class);
        OperatorSubtaskState operatorSubtaskState_2 = mock(OperatorSubtaskState.class);

        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);

        taskStateSnapshot.discardState();
        verify(operatorSubtaskState_1).discardState();
        verify(operatorSubtaskState_2).discardState();
    }

    @Test
    void getStateSize() {
        Random random = new Random(0x42);
        TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
        assertThat(taskStateSnapshot.getStateSize()).isZero();

        OperatorSubtaskState emptyOperatorSubtaskState = OperatorSubtaskState.builder().build();
        assertThat(emptyOperatorSubtaskState.hasState()).isFalse();
        taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), emptyOperatorSubtaskState);
        assertThat(taskStateSnapshot.getStateSize()).isZero();

        OperatorStateHandle stateHandle_1 =
                StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
        OperatorSubtaskState nonEmptyOperatorSubtaskState_1 =
                OperatorSubtaskState.builder().setManagedOperatorState(stateHandle_1).build();

        OperatorStateHandle stateHandle_2 =
                StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
        OperatorSubtaskState nonEmptyOperatorSubtaskState_2 =
                OperatorSubtaskState.builder().setRawOperatorState(stateHandle_2).build();

        taskStateSnapshot.putSubtaskStateByOperatorID(
                new OperatorID(), nonEmptyOperatorSubtaskState_1);
        taskStateSnapshot.putSubtaskStateByOperatorID(
                new OperatorID(), nonEmptyOperatorSubtaskState_2);

        long totalSize = stateHandle_1.getStateSize() + stateHandle_2.getStateSize();
        assertThat(taskStateSnapshot.getStateSize()).isEqualTo(totalSize);
    }

    @Test
    void testSizeIncludesChannelState() {
        final Random random = new Random();
        InputChannelStateHandle inputChannelStateHandle =
                createNewInputChannelStateHandle(10, random);
        ResultSubpartitionStateHandle resultSubpartitionStateHandle =
                createNewResultSubpartitionStateHandle(10, random);
        final TaskStateSnapshot taskStateSnapshot =
                new TaskStateSnapshot(
                        Collections.singletonMap(
                                new OperatorID(),
                                OperatorSubtaskState.builder()
                                        .setInputChannelState(singleton(inputChannelStateHandle))
                                        .setResultSubpartitionState(
                                                singleton(resultSubpartitionStateHandle))
                                        .build()));
        assertThat(taskStateSnapshot.getStateSize())
                .isEqualTo(
                        inputChannelStateHandle.getStateSize()
                                + resultSubpartitionStateHandle.getStateSize());
        assertThat(taskStateSnapshot.hasState()).isTrue();
    }
}
