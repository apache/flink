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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for {@link InputGate}. */
abstract class InputGateTestBase {

    int gateIndex;

    @BeforeEach
    void resetGateIndex() {
        gateIndex = 0;
    }

    protected void testIsAvailable(
            InputGate inputGateToTest,
            SingleInputGate inputGateToNotify,
            TestInputChannel inputChannelWithNewData)
            throws Exception {

        assertThat(inputGateToTest.getAvailableFuture()).isNotDone();
        assertThat(inputGateToTest.pollNext()).isNotPresent();

        CompletableFuture<?> future = inputGateToTest.getAvailableFuture();

        assertThat(inputGateToTest.getAvailableFuture()).isNotDone();
        assertThat(inputGateToTest.pollNext()).isNotPresent();

        assertThat(inputGateToTest.getAvailableFuture()).isEqualTo(future);

        inputChannelWithNewData.readBuffer();
        inputGateToNotify.notifyChannelNonEmpty(inputChannelWithNewData);

        assertThat(future).isDone();
        assertThat(inputGateToTest.getAvailableFuture())
                .isDone()
                .isEqualTo(PullingAsyncDataInput.AVAILABLE);
    }

    protected void testIsAvailableAfterFinished(
            InputGate inputGateToTest, Runnable endOfPartitionEvent) throws Exception {

        CompletableFuture<?> available = inputGateToTest.getAvailableFuture();
        assertThat(available).isNotDone();
        assertThat(inputGateToTest.pollNext()).isNotPresent();

        endOfPartitionEvent.run();

        assertThat(inputGateToTest.pollNext()).isNotEmpty(); // EndOfPartitionEvent

        assertThat(available).isDone();
        assertThat(inputGateToTest.getAvailableFuture()).isDone();
        assertThat(inputGateToTest.getAvailableFuture()).isEqualTo(PullingAsyncDataInput.AVAILABLE);
    }

    protected SingleInputGate createInputGate() {
        return createInputGate(2);
    }

    protected SingleInputGate createInputGate(int numberOfInputChannels) {
        return createInputGate(null, numberOfInputChannels, ResultPartitionType.PIPELINED);
    }

    protected SingleInputGate createInputGate(
            NettyShuffleEnvironment environment,
            int numberOfInputChannels,
            ResultPartitionType partitionType) {

        SingleInputGateBuilder builder =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfInputChannels)
                        .setSingleInputGateIndex(gateIndex++)
                        .setResultPartitionType(partitionType);

        if (environment != null) {
            builder = builder.setupBufferPoolFactory(environment);
        }

        SingleInputGate inputGate = builder.build();
        assertThat(inputGate.getConsumedPartitionType()).isEqualTo(partitionType);
        return inputGate;
    }
}
