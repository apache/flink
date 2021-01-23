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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.SourceOperator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link StreamTaskInput} that reads data from the {@link SourceOperator} and
 * returns the {@link InputStatus} to indicate whether the source state is available, unavailable or
 * finished.
 */
@Internal
public class StreamTaskSourceInput<T> implements StreamTaskInput<T>, CheckpointableInput {

    private final SourceOperator<T, ?> operator;
    private final int inputGateIndex;
    private final AvailabilityHelper isBlockedAvailability = new AvailabilityHelper();
    private final List<InputChannelInfo> inputChannelInfos;
    private final int inputIndex;

    public StreamTaskSourceInput(
            SourceOperator<T, ?> operator, int inputGateIndex, int inputIndex) {
        this.operator = checkNotNull(operator);
        this.inputGateIndex = inputGateIndex;
        inputChannelInfos = Collections.singletonList(new InputChannelInfo(inputGateIndex, 0));
        isBlockedAvailability.resetAvailable();
        this.inputIndex = inputIndex;
    }

    @Override
    public InputStatus emitNext(DataOutput<T> output) throws Exception {
        /**
         * Safe guard against best efforts availability checks. If despite being unavailable someone
         * polls the data from this source while it's blocked, it should return {@link
         * InputStatus.NOTHING_AVAILABLE}.
         */
        if (isBlockedAvailability.isApproximatelyAvailable()) {
            return operator.emitNext(output);
        }
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return isBlockedAvailability.and(operator);
    }

    @Override
    public void blockConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.resetUnavailable();
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) {
        isBlockedAvailability.getUnavailableToResetAvailable().complete(null);
    }

    @Override
    public List<InputChannelInfo> getChannelInfos() {
        return inputChannelInfos;
    }

    @Override
    public int getNumberOfInputChannels() {
        return inputChannelInfos.size();
    }

    /**
     * This method is used with unaligned checkpoints to mark the arrival of a first {@link
     * CheckpointBarrier}. For chained sources, there is no {@link CheckpointBarrier} per se flowing
     * through the job graph. We can assume that an imaginary {@link CheckpointBarrier} was produced
     * by the source, at any point of time of our choosing.
     *
     * <p>We are choosing to interpret it, that {@link CheckpointBarrier} for sources was received
     * immediately as soon as we receive either checkpoint start RPC, or {@link CheckpointBarrier}
     * from a network input. So that we can checkpoint state of the source and all of the other
     * operators at the same time.
     *
     * <p>Also we are choosing to block the source, as a best effort optimisation as: - either there
     * is no backpressure and the checkpoint "alignment" will happen very quickly anyway - or there
     * is a backpressure, and it's better to prioritize processing data from the network to speed up
     * checkpointing. From the cluster resource utilisation perspective, by blocking chained source
     * doesn't block any resources from being used, as this task running the source has a backlog of
     * buffered input data waiting to be processed.
     *
     * <p>However from the correctness point of view, {@link #checkpointStarted(CheckpointBarrier)}
     * and {@link #checkpointStopped(long)} methods could be empty no-op.
     */
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) {
        blockConsumption(null);
    }

    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        resumeConsumption(null);
    }

    @Override
    public int getInputGateIndex() {
        return inputGateIndex;
    }

    @Override
    public void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException {}

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public void close() {
        // SourceOperator is closed via OperatorChain
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) {
        return CompletableFuture.completedFuture(null);
    }

    public OperatorID getOperatorID() {
        return operator.getOperatorID();
    }
}
