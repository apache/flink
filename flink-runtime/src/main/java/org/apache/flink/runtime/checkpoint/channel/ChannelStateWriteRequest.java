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
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

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
        return new CheckpointInProgressRequest("completeInput", checkpointId, false) {

            @Override
            protected void executeInternal(ChannelStateCheckpointWriter writer) throws Exception {
                writer.completeInput();
            }
        };
    }

    static CheckpointInProgressRequest completeOutput(long checkpointId) {
        return new CheckpointInProgressRequest("completeOutput", checkpointId, false) {
            @Override
            protected void executeInternal(ChannelStateCheckpointWriter writer) throws Exception {
                writer.completeOutput();
            }
        };
    }

    static ChannelStateWriteRequest write(
            long checkpointId, InputChannelInfo info, CloseableIterator<Buffer> iterator) {
        return new ChannelStateWriteBuffersRequest("writeInput", checkpointId, iterator) {
            @Override
            protected void writeBuffer(ChannelStateCheckpointWriter writer, Buffer buffer)
                    throws Exception {
                writer.writeInput(info, buffer);
            }
        };
    }

    static ChannelStateWriteRequest write(
            long checkpointId, ResultSubpartitionInfo info, Buffer... buffers) {
        return new ChannelStateWriteBuffersRequest(
                "writeOutput", checkpointId, ofElements(Buffer::recycleBuffer, buffers)) {
            @Override
            protected void writeBuffer(ChannelStateCheckpointWriter writer, Buffer buffer)
                    throws Exception {
                writer.writeOutput(info, buffer);
            }
        };
    }

    static ChannelStateWriteRequest start(
            long checkpointId,
            ChannelStateWriteResult targetResult,
            CheckpointStorageLocationReference locationReference) {
        return new CheckpointStartRequest(checkpointId, targetResult, locationReference);
    }

    static ChannelStateWriteRequest abort(long checkpointId, Throwable cause) {
        return new CheckpointInProgressRequest("abort", checkpointId, true) {
            @Override
            protected void executeInternal(ChannelStateCheckpointWriter writer) throws Exception {
                writer.fail(cause);
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

abstract class CheckpointInProgressRequest implements ChannelStateWriteRequest {
    private final long checkpointId;
    private final String name;
    private final boolean ignoreMissingWriter;
    private final AtomicReference<CheckpointInProgressRequestState> state =
            new AtomicReference<>(NEW);

    CheckpointInProgressRequest(String name, long checkpointId, boolean ignoreMissingWriter) {
        this.checkpointId = checkpointId;
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
            discard(cause);
        }
    }

    void execute(ChannelStateCheckpointWriter channelStateCheckpointWriter) throws Exception {
        Preconditions.checkState(state.compareAndSet(NEW, EXECUTING));
        try {
            executeInternal(channelStateCheckpointWriter);
            state.set(COMPLETED);
        } catch (Exception e) {
            state.set(FAILED);
            throw e;
        }
    }

    protected abstract void executeInternal(ChannelStateCheckpointWriter writer) throws Exception;

    protected void discard(Throwable cause) throws Exception {}

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

abstract class ChannelStateWriteBuffersRequest extends CheckpointInProgressRequest {
    private final CloseableIterator<Buffer> iterator;

    public ChannelStateWriteBuffersRequest(
            String name, long checkpointId, CloseableIterator<Buffer> iterator) {
        super(name, checkpointId, false);
        this.iterator = iterator;
    }

    @Override
    protected void executeInternal(ChannelStateCheckpointWriter writer) throws Exception {
        while (iterator.hasNext()) {
            Buffer buffer = iterator.next();
            try {
                checkArgument(buffer.isBuffer());
            } catch (Exception e) {
                buffer.recycleBuffer();
                throw e;
            }
            writeBuffer(writer, buffer);
        }
    }

    protected abstract void writeBuffer(ChannelStateCheckpointWriter writer, Buffer buffer)
            throws Exception;

    @Override
    protected void discard(Throwable cause) throws Exception {
        iterator.close();
    }
}
