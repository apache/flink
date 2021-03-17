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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.CANCELLED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.COMPLETED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.EXECUTING;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.FAILED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.NEW;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

interface ChannelStateWriteRequest {
    long getCheckpointId();

    void cancel(Throwable cause) throws Exception;

    static CheckpointInProgressRequest completeInput(long checkpointId) {
        return new CheckpointInProgressRequest(
                "completeInput", checkpointId, ChannelStateCheckpointWriter::completeInput, false);
    }

    static CheckpointInProgressRequest completeOutput(long checkpointId) {
        return new CheckpointInProgressRequest(
                "completeOutput",
                checkpointId,
                ChannelStateCheckpointWriter::completeOutput,
                false);
    }

    static ChannelStateWriteRequest write(
            long checkpointId, InputChannelInfo info, CloseableIterator<Buffer> iterator) {
        return buildWriteRequest(
                checkpointId,
                "ChannelStateWriteRequest#writeInput",
                info,
                iterator,
                (writer, buffer) -> writer.writeInput(info, buffer));
    }

    static ChannelStateWriteRequest write(
            long checkpointId, ResultSubpartitionInfo info, Buffer... buffers) {
        return buildWriteRequest(
                checkpointId,
                "ChannelStateWriteRequest#writeOutput",
                info,
                ofElements(Buffer::recycleBuffer, buffers),
                (writer, buffer) -> writer.writeOutput(info, buffer));
    }

    static ChannelStateWriteRequest buildWriteRequest(
            long checkpointId,
            String name,
            Object channelInfo,
            CloseableIterator<Buffer> iterator,
            BiConsumerWithException<ChannelStateCheckpointWriter, Buffer, Exception>
                    bufferConsumer) {
        return new CheckpointInProgressRequest(
                name,
                checkpointId,
                writer -> {
                    while (iterator.hasNext()) {
                        Buffer buffer = iterator.next();
                        NetworkActionsLogger.tracePersist(name, buffer, channelInfo, checkpointId);
                        try {
                            checkArgument(buffer.isBuffer());
                        } catch (Exception e) {
                            buffer.recycleBuffer();
                            throw e;
                        }
                        bufferConsumer.accept(writer, buffer);
                    }
                },
                throwable -> iterator.close(),
                false);
    }

    static ChannelStateWriteRequest start(
            long checkpointId,
            ChannelStateWriteResult targetResult,
            CheckpointStorageLocationReference locationReference) {
        return new CheckpointStartRequest(checkpointId, targetResult, locationReference);
    }

    static ChannelStateWriteRequest abort(long checkpointId, Throwable cause) {
        return new CheckpointInProgressRequest(
                "abort", checkpointId, writer -> writer.fail(cause), true);
    }

    static ThrowingConsumer<Throwable, Exception> recycle(Buffer[] flinkBuffers) {
        return unused -> {
            for (Buffer b : flinkBuffers) {
                b.recycleBuffer();
            }
        };
    }
}

final class CheckpointStartRequest implements ChannelStateWriteRequest {
    private final ChannelStateWriteResult targetResult;
    private final CheckpointStorageLocationReference locationReference;
    private final long checkpointId;

    CheckpointStartRequest(
            long checkpointId,
            ChannelStateWriteResult targetResult,
            CheckpointStorageLocationReference locationReference) {
        this.checkpointId = checkpointId;
        this.targetResult = checkNotNull(targetResult);
        this.locationReference = checkNotNull(locationReference);
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    ChannelStateWriteResult getTargetResult() {
        return targetResult;
    }

    public CheckpointStorageLocationReference getLocationReference() {
        return locationReference;
    }

    @Override
    public void cancel(Throwable cause) {
        targetResult.fail(cause);
    }

    @Override
    public String toString() {
        return "start " + checkpointId;
    }
}

enum CheckpointInProgressRequestState {
    NEW,
    EXECUTING,
    COMPLETED,
    FAILED,
    CANCELLED
}

final class CheckpointInProgressRequest implements ChannelStateWriteRequest {
    private final ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action;
    private final ThrowingConsumer<Throwable, Exception> discardAction;
    private final long checkpointId;
    private final String name;
    private final boolean ignoreMissingWriter;
    private final AtomicReference<CheckpointInProgressRequestState> state =
            new AtomicReference<>(NEW);

    CheckpointInProgressRequest(
            String name,
            long checkpointId,
            ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action,
            boolean ignoreMissingWriter) {
        this(name, checkpointId, action, unused -> {}, ignoreMissingWriter);
    }

    CheckpointInProgressRequest(
            String name,
            long checkpointId,
            ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action,
            ThrowingConsumer<Throwable, Exception> discardAction,
            boolean ignoreMissingWriter) {
        this.checkpointId = checkpointId;
        this.action = checkNotNull(action);
        this.discardAction = checkNotNull(discardAction);
        this.name = checkNotNull(name);
        this.ignoreMissingWriter = ignoreMissingWriter;
    }

    @Override
    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public void cancel(Throwable cause) throws Exception {
        if (state.compareAndSet(NEW, CANCELLED) || state.compareAndSet(FAILED, CANCELLED)) {
            discardAction.accept(cause);
        }
    }

    void execute(ChannelStateCheckpointWriter channelStateCheckpointWriter) throws Exception {
        Preconditions.checkState(state.compareAndSet(NEW, EXECUTING));
        try {
            action.accept(channelStateCheckpointWriter);
            state.set(COMPLETED);
        } catch (Exception e) {
            state.set(FAILED);
            throw e;
        }
    }

    void onWriterMissing() {
        if (!ignoreMissingWriter) {
            throw new IllegalArgumentException(
                    "writer not found while processing request: " + toString());
        }
    }

    @Override
    public String toString() {
        return name + " " + checkpointId;
    }
}
