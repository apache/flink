/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateSampleOperatorSubtaskState;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link OperatorSubtaskState} test. */
class OperatorSubtaskStateTest {

    @Test
    void testDiscardDuplicatedDelegatesOnce() {
        StreamStateHandle delegate = new DiscardOnceStreamStateHandle();
        OperatorSubtaskState.builder()
                .setInputChannelState(
                        new StateObjectCollection<>(
                                asList(
                                        buildInputChannelHandle(delegate, 1),
                                        buildInputChannelHandle(delegate, 2))))
                .setResultSubpartitionState(
                        new StateObjectCollection<>(
                                asList(
                                        buildSubpartitionHandle(delegate, 4),
                                        buildSubpartitionHandle(delegate, 3))))
                .build()
                .discardState();
    }

    @Test
    void testToBuilderCorrectness() throws IOException {
        // given: Initialized operator subtask state.
        OperatorSubtaskState operatorSubtaskState = generateSampleOperatorSubtaskState().f1;

        // when: Copy the operator subtask state.
        OperatorSubtaskState operatorSubtaskStateCopy = operatorSubtaskState.toBuilder().build();

        // then: It should be equal to original one.
        assertThat(reflectionEquals(operatorSubtaskState, operatorSubtaskStateCopy)).isTrue();
    }

    @Test
    void testGetDiscardables() throws IOException {
        Tuple2<List<StateObject>, OperatorSubtaskState> opStates =
                generateSampleOperatorSubtaskState();
        List<StateObject> states = opStates.f0;
        OperatorSubtaskState operatorSubtaskState = opStates.f1;
        List<StateObject> discardables =
                Arrays.asList(
                        states.get(0),
                        states.get(1),
                        states.get(2),
                        states.get(3),
                        ((InputChannelStateHandle) states.get(4)).getDelegate(),
                        ((ResultSubpartitionStateHandle) states.get(5)).getDelegate());
        assertThat(new HashSet<>(operatorSubtaskState.getDiscardables()))
                .isEqualTo(new HashSet<>(discardables));
    }

    private ResultSubpartitionStateHandle buildSubpartitionHandle(
            StreamStateHandle delegate, int subPartitionIdx1) {
        return new ResultSubpartitionStateHandle(
                new ResultSubpartitionInfo(0, subPartitionIdx1), delegate, singletonList(0L));
    }

    private InputChannelStateHandle buildInputChannelHandle(
            StreamStateHandle delegate, int inputChannelIdx) {
        return new InputChannelStateHandle(
                new InputChannelInfo(0, inputChannelIdx), delegate, singletonList(0L));
    }

    private static class DiscardOnceStreamStateHandle extends ByteStreamStateHandle {
        private static final long serialVersionUID = 1L;

        private boolean discarded = false;

        DiscardOnceStreamStateHandle() {
            super("test", new byte[0]);
        }

        @Override
        public void discardState() {
            super.discardState();
            assertThat(discarded).as("state was discarded twice").isFalse();
            discarded = true;
        }
    }
}
