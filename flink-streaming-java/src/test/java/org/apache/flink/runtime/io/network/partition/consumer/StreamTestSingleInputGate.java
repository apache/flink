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

// We have it in this package because we could not mock the methods otherwise

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel.BufferAndAvailabilityProvider;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;

/**
 * Test {@link InputGate} that allows setting multiple channels. Use {@link #sendElement(Object,
 * int)} to offer an element on a specific channel. Use {@link #sendEvent(AbstractEvent, int)} to
 * offer an event on the specified channel. Use {@link #endInput()} to notify all channels of input
 * end.
 */
public class StreamTestSingleInputGate<T> {

    private final int numInputChannels;

    private final SingleInputGate inputGate;
    private final TestInputChannel[] inputChannels;

    private final int bufferSize;

    private TypeSerializer<T> serializer;

    private ConcurrentLinkedQueue<InputValue<Object>>[] inputQueues;

    @SuppressWarnings("unchecked")
    public StreamTestSingleInputGate(
            int numInputChannels, int gateIndex, TypeSerializer<T> serializer, int bufferSize) {
        this(numInputChannels, gateIndex, serializer, bufferSize, new SingleInputGateBuilder());
    }

    public StreamTestSingleInputGate(
            int numInputChannels,
            int gateIndex,
            TypeSerializer<T> serializer,
            int bufferSize,
            SingleInputGateBuilder preconfiguredBuilder) {

        this.bufferSize = bufferSize;
        this.serializer = serializer;

        this.numInputChannels = numInputChannels;
        this.inputQueues = new ConcurrentLinkedQueue[numInputChannels];
        this.inputGate =
                preconfiguredBuilder
                        .setNumberOfChannels(numInputChannels)
                        .setSingleInputGateIndex(gateIndex)
                        .build();
        this.inputChannels = setupInputChannels();
        this.inputGate.setInputChannels(inputChannels);
    }

    private TestInputChannel[] setupInputChannels() {
        TestInputChannel[] inputChannels = new TestInputChannel[numInputChannels];
        for (int i = 0; i < numInputChannels; i++) {
            final int channelIndex = i;
            final DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
            final SerializationDelegate<StreamElement> delegate =
                    new SerializationDelegate<>(new StreamElementSerializer<T>(serializer));

            inputQueues[channelIndex] = new ConcurrentLinkedQueue<>();
            inputChannels[channelIndex] = new TestInputChannel(inputGate, i);

            final BufferAndAvailabilityProvider answer =
                    () -> {
                        ConcurrentLinkedQueue<InputValue<Object>> inputQueue =
                                inputQueues[channelIndex];
                        InputValue<Object> input;
                        Buffer.DataType nextType;
                        synchronized (inputQueue) {
                            input = inputQueue.poll();
                            nextType =
                                    !inputQueue.isEmpty()
                                            ? Buffer.DataType.DATA_BUFFER
                                            : Buffer.DataType.NONE;
                        }
                        if (input != null && input.isStreamEnd()) {
                            inputChannels[channelIndex].setReleased();
                            return Optional.of(
                                    new BufferAndAvailability(
                                            EventSerializer.toBuffer(
                                                    EndOfPartitionEvent.INSTANCE, false),
                                            nextType,
                                            0,
                                            0));
                        } else if (input != null && input.isDataEnd()) {
                            return Optional.of(
                                    new BufferAndAvailability(
                                            EventSerializer.toBuffer(
                                                    new EndOfData(StopMode.DRAIN), false),
                                            nextType,
                                            0,
                                            0));
                        } else if (input != null && input.isStreamRecord()) {
                            StreamElement inputElement = input.getStreamRecord();

                            delegate.setInstance(inputElement);
                            ByteBuffer serializedRecord =
                                    RecordWriter.serializeRecord(dataOutputSerializer, delegate);
                            BufferBuilder bufferBuilder = createBufferBuilder(bufferSize);
                            BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
                            bufferBuilder.appendAndCommit(serializedRecord);
                            bufferBuilder.finish();
                            bufferBuilder.close();

                            // Call getCurrentBuffer to ensure size is set
                            return Optional.of(
                                    new BufferAndAvailability(
                                            bufferConsumer.build(), nextType, 0, 0));
                        } else if (input != null && input.isEvent()) {
                            AbstractEvent event = input.getEvent();
                            if (event instanceof EndOfPartitionEvent) {
                                inputChannels[channelIndex].setReleased();
                            }

                            return Optional.of(
                                    new BufferAndAvailability(
                                            EventSerializer.toBuffer(event, false),
                                            nextType,
                                            0,
                                            0));
                        } else {
                            return Optional.empty();
                        }
                    };

            inputChannels[channelIndex].addBufferAndAvailability(answer);
        }
        return inputChannels;
    }

    public SingleInputGate getInputGate() {
        return inputGate;
    }

    public void sendElement(Object element, int channel) {
        synchronized (inputQueues[channel]) {
            inputQueues[channel].add(InputValue.element(element));
            inputQueues[channel].notifyAll();
        }
        inputGate.notifyChannelNonEmpty(inputChannels[channel]);
    }

    public void sendEvent(AbstractEvent event, int channel) {
        synchronized (inputQueues[channel]) {
            inputQueues[channel].add(InputValue.event(event));
            inputQueues[channel].notifyAll();
        }
        inputGate.notifyChannelNonEmpty(inputChannels[channel]);
    }

    public void endInput() {
        endInput(true);
    }

    public void endInput(boolean emitEndOfData) {
        for (int i = 0; i < numInputChannels; i++) {
            synchronized (inputQueues[i]) {
                if (emitEndOfData) {
                    inputQueues[i].add(InputValue.dataEnd());
                }
                inputQueues[i].add(InputValue.streamEnd());
                inputQueues[i].notifyAll();
            }
            inputGate.notifyChannelNonEmpty(inputChannels[i]);
        }
    }

    /** Returns true iff all input queues are empty. */
    public boolean allQueuesEmpty() {
        for (int i = 0; i < numInputChannels; i++) {
            if (inputQueues[i].size() > 0) {
                return false;
            }
        }
        return true;
    }

    private static class InputValue<T> {
        private final Object elementOrEvent;
        private final boolean isStreamEnd;
        private final boolean isStreamRecord;
        private final boolean isEvent;
        private final boolean isDataEnd;

        private InputValue(
                Object elementOrEvent,
                boolean isStreamEnd,
                boolean isDataEnd,
                boolean isEvent,
                boolean isStreamRecord) {
            this.elementOrEvent = elementOrEvent;
            this.isStreamEnd = isStreamEnd;
            this.isStreamRecord = isStreamRecord;
            this.isEvent = isEvent;
            this.isDataEnd = isDataEnd;
        }

        public static <X> InputValue<X> element(Object element) {
            return new InputValue<X>(element, false, false, false, true);
        }

        public static <X> InputValue<X> dataEnd() {
            return new InputValue<X>(null, false, true, false, false);
        }

        public static <X> InputValue<X> streamEnd() {
            return new InputValue<X>(null, true, false, false, false);
        }

        public static <X> InputValue<X> event(AbstractEvent event) {
            return new InputValue<X>(event, false, false, true, false);
        }

        public StreamElement getStreamRecord() {
            return (StreamElement) elementOrEvent;
        }

        public AbstractEvent getEvent() {
            return (AbstractEvent) elementOrEvent;
        }

        public boolean isStreamEnd() {
            return isStreamEnd;
        }

        public boolean isStreamRecord() {
            return isStreamRecord;
        }

        public boolean isEvent() {
            return isEvent;
        }

        public boolean isDataEnd() {
            return isDataEnd;
        }
    }
}
