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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel.BufferAndAvailabilityProvider;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;

/**
 * Input gate helper for unit tests.
 *
 * @param <T> type of the value to handle
 */
public class IteratorWrappingTestSingleInputGate<T extends IOReadableWritable>
        extends TestSingleInputGate {

    private final TestInputChannel inputChannel = new TestInputChannel(inputGate, 0);

    private final int bufferSize;

    private MutableObjectIterator<T> inputIterator;

    private DataOutputSerializer serializer;

    private final T reuse;

    public IteratorWrappingTestSingleInputGate(
            int bufferSize, int gateIndex, MutableObjectIterator<T> iterator, Class<T> recordType)
            throws IOException, InterruptedException {
        super(1, gateIndex, false);

        this.bufferSize = bufferSize;
        this.reuse = InstantiationUtil.instantiate(recordType);

        wrapIterator(iterator);
    }

    private IteratorWrappingTestSingleInputGate<T> wrapIterator(MutableObjectIterator<T> iterator)
            throws IOException, InterruptedException {
        inputIterator = iterator;
        serializer = new DataOutputSerializer(128);

        // The input iterator can produce an infinite stream. That's why we have to serialize each
        // record on demand and cannot do it upfront.
        final BufferAndAvailabilityProvider answer =
                new BufferAndAvailabilityProvider() {

                    private boolean hasData = inputIterator.next(reuse) != null;

                    @Override
                    public Optional<BufferAndAvailability> getBufferAvailability()
                            throws IOException {
                        if (hasData) {
                            ByteBuffer serializedRecord =
                                    RecordWriter.serializeRecord(serializer, reuse);
                            BufferBuilder bufferBuilder = createBufferBuilder(bufferSize);
                            BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
                            bufferBuilder.appendAndCommit(serializedRecord);

                            hasData = inputIterator.next(reuse) != null;

                            // Call getCurrentBuffer to ensure size is set
                            final Buffer.DataType nextDataType =
                                    hasData
                                            ? Buffer.DataType.DATA_BUFFER
                                            : Buffer.DataType.EVENT_BUFFER;
                            return Optional.of(
                                    new BufferAndAvailability(
                                            bufferConsumer.build(), nextDataType, 0, 0));
                        } else {
                            inputChannel.setReleased();

                            return Optional.of(
                                    new BufferAndAvailability(
                                            EventSerializer.toBuffer(
                                                    EndOfPartitionEvent.INSTANCE, false),
                                            Buffer.DataType.NONE,
                                            0,
                                            0));
                        }
                    }
                };

        inputChannel.addBufferAndAvailability(answer);

        inputGate.setInputChannels(inputChannel);

        return this;
    }

    public IteratorWrappingTestSingleInputGate<T> notifyNonEmpty() {
        inputGate.notifyChannelNonEmpty(inputChannel);

        return this;
    }
}
