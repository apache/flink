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
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.plugable.SerializationDelegate;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

/** represent PipielinedSubpartition location, local or remoteOrUnknown. */
@Internal
public final class LocalPipelinedSubpartitionReadWriteMode
        implements PipelinedSubpartitionReadWriteMode {

    private final PipelinedSubpartition subPartition;

    private final BufferWritingResultPartition resultPartition;

    private boolean skipPollFromBuffers = false;

    LocalPipelinedSubpartitionReadWriteMode(PipelinedSubpartition subPartition) {
        this.subPartition = subPartition;
        this.resultPartition = (BufferWritingResultPartition) subPartition.parent;
    }

    @Override
    public void add(
            AbstractEvent event, BufferConsumer eventBufferConsumer, int partialRecordLength) {
        addInternal(event);
    }

    @Override
    public void addPriorityEvent(
            AbstractEvent event, BufferConsumer eventBufferConsumer, int partialRecordLength) {
        addInternal(event);
    }

    // todo: resultPartition totalWrittenBytes need to be calculated when record is added
    @Override
    public void add(SerializationDelegate<?> record) {
        addInternal(record.getInstance());
    }

    @Override
    public void add(
            SerializationDelegate<?> record,
            BufferConsumer bufferConsumer,
            int partialRecordLength) {
        addInternal(record.getInstance());
    }

    @Override
    public void add(SerializationDelegate<?> record, ByteBuffer recordBuffer) {
        addInternal(record.getInstance());
    }

    @Nullable
    @Override
    public ResultSubpartition.EventOrRecordOrBufferAndBacklog pollNext() {
        ResultSubpartition.EventOrRecordOrBufferAndBacklog next;
        if (!skipPollFromBuffers) {
            next = subPartition.pollBufferFromBuffers();
            if (next != null) {
                return next;
            } else {
                skipPollFromBuffers = true;
            }
        }
        return subPartition.pollEventOrRecord();
    }

    @Override
    public boolean blockWaitingAndTryToAdd(
            SerializationDelegate<?> record, ByteBuffer recordBuffer) {
        return blockWaitingAndTryToAdd(record);
    }

    @Override
    public boolean blockWaitingAndTryToAdd(
            SerializationDelegate<?> record,
            BufferConsumer bufferConsumer,
            int partialRecordLength) {
        return blockWaitingAndTryToAdd(record);
    }

    @Override
    public boolean blockWaitingAndTryToAdd(SerializationDelegate<?> record) {
        try {
            subPartition.recordOrEvents.blockingWaitingForAdd(
                    record, resultPartition.getHardBackPressuredTimeMsPerSecond());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public void addInternal(Object record) {
        subPartition.recordOrEvents.addAndGetSize(record);
    }
}
