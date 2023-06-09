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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.DATA_BUFFER;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.NONE;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.PRIORITIZED_EVENT_BUFFER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NettyConnectionReader}. */
class NettyConnectionReaderTest {

    private static final int INPUT_CHANNEL_INDEX = 0;

    private CompletableFuture<Tuple2<Integer, Boolean>> availableAndPriorityConsumer;

    private CompletableFuture<Tuple2<Integer, Integer>> prioritySequenceNumberConsumer;

    private CompletableFuture<Integer> requiredSegmentIdFuture;

    @BeforeEach
    void before() {
        availableAndPriorityConsumer = new CompletableFuture<>();
        prioritySequenceNumberConsumer = new CompletableFuture<>();
        requiredSegmentIdFuture = new CompletableFuture<>();
    }

    @Test
    void testReadBufferOfNonPriorityDataType() {
        int bufferNumber = 1;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, false, requiredSegmentIdFuture);
        NettyConnectionReader reader =
                createNettyConnectionReader(
                        inputChannelSupplier,
                        createAvailableAndPriorityConsumer(availableAndPriorityConsumer),
                        createPrioritySequenceNumberConsumer(prioritySequenceNumberConsumer));
        Optional<Buffer> buffer = reader.readBuffer(0);
        assertThat(buffer).isPresent();
        assertThat(buffer.get().isBuffer()).isTrue();
        assertThat(requiredSegmentIdFuture).isNotDone();
        assertThat(availableAndPriorityConsumer).isNotDone();
        assertThat(prioritySequenceNumberConsumer).isNotDone();
    }

    @Test
    void testReadBufferOfPriorityDataType() throws ExecutionException, InterruptedException {
        int bufferNumber = 2;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, true, requiredSegmentIdFuture);
        NettyConnectionReader reader =
                createNettyConnectionReader(
                        inputChannelSupplier,
                        createAvailableAndPriorityConsumer(availableAndPriorityConsumer),
                        createPrioritySequenceNumberConsumer(prioritySequenceNumberConsumer));
        Optional<Buffer> buffer = reader.readBuffer(0);
        assertThat(buffer).isPresent();
        assertThat(buffer.get().isBuffer()).isFalse();
        assertThat(requiredSegmentIdFuture).isNotDone();
        Tuple2<Integer, Boolean> result1 = availableAndPriorityConsumer.get();
        assertThat(result1.f0).isEqualTo(INPUT_CHANNEL_INDEX);
        assertThat(result1.f1).isEqualTo(true);
        Tuple2<Integer, Integer> result2 = prioritySequenceNumberConsumer.get();
        assertThat(result2.f0).isEqualTo(INPUT_CHANNEL_INDEX);
        assertThat(result2.f1).isEqualTo(0);
    }

    @Test
    void testReadEmptyBuffer() {
        int bufferNumber = 0;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, false, requiredSegmentIdFuture);
        NettyConnectionReader reader =
                createNettyConnectionReader(
                        inputChannelSupplier,
                        (inputChannelIndex, priority) ->
                                availableAndPriorityConsumer.complete(
                                        Tuple2.of(inputChannelIndex, priority)),
                        (inputChannelIndex, sequenceNumber) ->
                                prioritySequenceNumberConsumer.complete(
                                        Tuple2.of(inputChannelIndex, sequenceNumber)));
        Optional<Buffer> buffer = reader.readBuffer(0);
        assertThat(buffer).isNotPresent();
        assertThat(requiredSegmentIdFuture).isNotDone();
        assertThat(availableAndPriorityConsumer).isNotDone();
        assertThat(prioritySequenceNumberConsumer).isNotDone();
    }

    @Test
    void testReadDifferentSegments() throws ExecutionException, InterruptedException {
        int bufferNumber = 0;
        Supplier<InputChannel> inputChannelSupplier =
                createInputChannelSupplier(bufferNumber, false, requiredSegmentIdFuture);
        NettyConnectionReader reader =
                createNettyConnectionReader(
                        inputChannelSupplier,
                        createAvailableAndPriorityConsumer(availableAndPriorityConsumer),
                        createPrioritySequenceNumberConsumer(prioritySequenceNumberConsumer));
        reader.readBuffer(0);
        assertThat(requiredSegmentIdFuture).isNotDone();
        reader.readBuffer(1);
        assertThat(requiredSegmentIdFuture.get()).isEqualTo(1);
    }

    private static BiConsumer<Integer, Boolean> createAvailableAndPriorityConsumer(
            CompletableFuture<Tuple2<Integer, Boolean>> availableAndPriorityConsumer) {
        return (inputChannelIndex, priority) ->
                availableAndPriorityConsumer.complete(Tuple2.of(inputChannelIndex, priority));
    }

    private static BiConsumer<Integer, Integer> createPrioritySequenceNumberConsumer(
            CompletableFuture<Tuple2<Integer, Integer>> prioritySequenceNumberConsumer) {
        return (inputChannelIndex, sequenceNumber) ->
                prioritySequenceNumberConsumer.complete(
                        Tuple2.of(inputChannelIndex, sequenceNumber));
    }

    private static Supplier<InputChannel> createInputChannelSupplier(
            int bufferNumber,
            boolean priority,
            CompletableFuture<Integer> requiredSegmentIdFuture) {
        TestInputChannel inputChannel =
                new TestInputChannel(
                        new SingleInputGateBuilder().build(),
                        INPUT_CHANNEL_INDEX,
                        requiredSegmentIdFuture);
        try {
            for (int index = 0; index < bufferNumber; ++index) {
                inputChannel.read(
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(0),
                                FreeingBufferRecycler.INSTANCE,
                                priority ? PRIORITIZED_EVENT_BUFFER : DATA_BUFFER),
                        index == bufferNumber - 1
                                ? NONE
                                : priority ? PRIORITIZED_EVENT_BUFFER : DATA_BUFFER);
            }
        } catch (IOException | InterruptedException e) {
            ExceptionUtils.rethrow(e, "Failed to create test input channel.");
        }
        return () -> inputChannel;
    }

    private static NettyConnectionReader createNettyConnectionReader(
            Supplier<InputChannel> inputChannelSupplier,
            BiConsumer<Integer, Boolean> availableAndPriorityConsumer,
            BiConsumer<Integer, Integer> prioritySequenceNumberConsumer) {
        TestingNettyConnectionReaderAvailabilityAndPriorityHelper.Builder builder =
                new TestingNettyConnectionReaderAvailabilityAndPriorityHelper.Builder()
                        .setAvailableAndPriorityConsumer(availableAndPriorityConsumer)
                        .setPrioritySequenceNumberConsumer(prioritySequenceNumberConsumer);
        return new NettyConnectionReaderImpl(
                INPUT_CHANNEL_INDEX, inputChannelSupplier, builder.build());
    }
}
