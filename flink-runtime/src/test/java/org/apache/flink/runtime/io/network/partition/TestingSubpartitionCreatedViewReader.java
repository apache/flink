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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

/** Testing view reader for partition request notifier. */
public class TestingSubpartitionCreatedViewReader implements NetworkSequenceViewReader {
    private final InputChannelID receiverId;
    private final Consumer<PartitionRequestListener> partitionRequestListenerTimeoutConsumer;
    private final Consumer<Tuple2<ResultPartition, Integer>> notifySubpartitionCreatedConsumer;

    private TestingSubpartitionCreatedViewReader(
            InputChannelID receiverId,
            Consumer<PartitionRequestListener> partitionRequestListenerTimeoutConsumer,
            Consumer<Tuple2<ResultPartition, Integer>> notifySubpartitionCreatedConsumer) {
        this.receiverId = receiverId;
        this.partitionRequestListenerTimeoutConsumer = partitionRequestListenerTimeoutConsumer;
        this.notifySubpartitionCreatedConsumer = notifySubpartitionCreatedConsumer;
    }

    @Override
    public void notifySubpartitionCreated(ResultPartition partition, int subPartitionIndex)
            throws IOException {
        notifySubpartitionCreatedConsumer.accept(Tuple2.of(partition, subPartitionIndex));
    }

    @Override
    public void requestSubpartitionViewOrRegisterListener(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public InputChannel.BufferAndAvailability getNextBuffer() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean needAnnounceBacklog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCredit(int creditDeltas) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledgeAllRecordsProcessed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReleased() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseAllResources() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Throwable getFailureCause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        partitionRequestListenerTimeoutConsumer.accept(partitionRequestListener);
    }

    public static TestingSubpartitionCreatedViewReaderBuilder newBuilder() {
        return new TestingSubpartitionCreatedViewReaderBuilder();
    }

    /** Builder for {@link TestingSubpartitionCreatedViewReader}. */
    public static class TestingSubpartitionCreatedViewReaderBuilder {
        private InputChannelID receiverId;
        private Consumer<PartitionRequestListener> partitionRequestListenerTimeoutConsumer =
                listener -> {};
        private Consumer<Tuple2<ResultPartition, Integer>> notifySubpartitionCreatedConsumer =
                tuple -> {};

        public TestingSubpartitionCreatedViewReaderBuilder setReceiverId(
                InputChannelID receiverId) {
            this.receiverId = receiverId;
            return this;
        }

        public TestingSubpartitionCreatedViewReaderBuilder
                setPartitionRequestListenerTimeoutConsumer(
                        Consumer<PartitionRequestListener>
                                partitionRequestListenerTimeoutConsumer) {
            this.partitionRequestListenerTimeoutConsumer = partitionRequestListenerTimeoutConsumer;
            return this;
        }

        public TestingSubpartitionCreatedViewReaderBuilder setNotifySubpartitionCreatedConsumer(
                Consumer<Tuple2<ResultPartition, Integer>> notifySubpartitionCreatedConsumer) {
            this.notifySubpartitionCreatedConsumer = notifySubpartitionCreatedConsumer;
            return this;
        }

        public TestingSubpartitionCreatedViewReader build() {
            return new TestingSubpartitionCreatedViewReader(
                    receiverId,
                    partitionRequestListenerTimeoutConsumer,
                    notifySubpartitionCreatedConsumer);
        }
    }
}
