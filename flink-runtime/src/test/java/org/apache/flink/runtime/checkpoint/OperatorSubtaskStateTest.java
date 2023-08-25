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

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generateKeyGroupState;
import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.generatePartitionableStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
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
        JobVertexID jobVertexID = new JobVertexID();
        int index = 0;
        Random random = new Random();

        OperatorSubtaskState operatorSubtaskState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(
                                generatePartitionableStateHandle(jobVertexID, index, 2, 8, false))
                        .setRawOperatorState(
                                generatePartitionableStateHandle(jobVertexID, index, 2, 8, true))
                        .setManagedKeyedState(
                                generateKeyGroupState(jobVertexID, new KeyGroupRange(0, 11), false))
                        .setRawKeyedState(
                                generateKeyGroupState(jobVertexID, new KeyGroupRange(0, 9), true))
                        .setInputChannelState(
                                StateObjectCollection.singleton(
                                        createNewInputChannelStateHandle(3, random)))
                        .setResultSubpartitionState(
                                StateObjectCollection.singleton(
                                        createNewResultSubpartitionStateHandle(3, random)))
                        .setInputRescalingDescriptor(
                                InflightDataRescalingDescriptorUtil.rescalingDescriptor(
                                        new int[1],
                                        new RescaleMappings[0],
                                        Collections.singleton(1)))
                        .setOutputRescalingDescriptor(
                                InflightDataRescalingDescriptorUtil.rescalingDescriptor(
                                        new int[1],
                                        new RescaleMappings[0],
                                        Collections.singleton(2)))
                        .build();

        // when: Copy the operator subtask state.
        OperatorSubtaskState operatorSubtaskStateCopy = operatorSubtaskState.toBuilder().build();

        // then: It should be equal to original one.
        assertThat(reflectionEquals(operatorSubtaskState, operatorSubtaskStateCopy)).isTrue();
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
