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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/** represent PipielinedSubpartition location, local or remoteOrUnknown. */
@Internal
public final class UnknownOrRemotePipelinedSubpartitionReadWriteMode
        implements PipelinedSubpartitionReadWriteMode {

    private final PipelinedSubpartition subPartition;

    private final BufferWritingResultPartition resultPartition;

    UnknownOrRemotePipelinedSubpartitionReadWriteMode(PipelinedSubpartition subPartition) {
        this.subPartition = subPartition;
        this.resultPartition = (BufferWritingResultPartition) subPartition.parent;
    }

    @Override
    public void add(
            AbstractEvent event, BufferConsumer eventBufferConsumer, int partialRecordLength) {
        subPartition.buffers.add(
                new BufferConsumerWithPartialRecordLength(
                        eventBufferConsumer, partialRecordLength));
    }

    @Override
    public void addPriorityEvent(
            AbstractEvent event, BufferConsumer eventBufferConsumer, int partialRecordLength) {
        subPartition.buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(
                        eventBufferConsumer, partialRecordLength));
    }

    // todo: resultPartition totalWrittenBytes need to be calculated when record is added
    @Override
    public void add(SerializationDelegate<?> record) {
        System.out.printf("call add method in unknown or remote ");
        ByteBuffer recordBuffer = null;
        try {
            recordBuffer = RecordWriter.serializeRecord(subPartition.serializer, record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        add(record, recordBuffer);
    }

    @Override
    public void add(
            SerializationDelegate<?> record,
            BufferConsumer bufferConsumer,
            int partialRecordLength) {
        subPartition.add(bufferConsumer, partialRecordLength);
    }

    @Override
    public void add(SerializationDelegate<?> record, ByteBuffer recordBuffer) {
        // todo: need to be fix，this will cause the block in buffers to request BufferBuilder
        try {
            resultPartition.emitRecord(recordBuffer, subPartition.getSubPartitionIndex());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nullable
    @Override
    public ResultSubpartition.EventOrRecordOrBufferAndBacklog pollNext() {
        return subPartition.pollBufferFromBuffers();
    }
}
