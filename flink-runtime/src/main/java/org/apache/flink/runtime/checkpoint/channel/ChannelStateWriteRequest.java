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
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.CANCELLED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.COMPLETED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.EXECUTING;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.FAILED;
import static org.apache.flink.runtime.checkpoint.channel.CheckpointInProgressRequestState.NEW;
import static org.apache.flink.util.CloseableIterator.ofElements;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

abstract class ChannelStateWriteRequest {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateWriteRequest.class);

    private final JobVertexID jobVertexID;

    private final int subtaskIndex;

    private final long checkpointId;

    private final String name;

    public ChannelStateWriteRequest(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId, String name) {
        this.jobVertexID = jobVertexID;
        this.subtaskIndex = subtaskIndex;
        this.checkpointId = checkpointId;
        this.name = name;
    }

    public final JobVertexID getJobVertexID() {
        return jobVertexID;
    }

    public final int getSubtaskIndex() {
        return subtaskIndex;
    }

    public final long getCheckpointId() {
        return checkpointId;
    }

    /**
     * It means whether the request is ready, e.g: some requests write the channel state data
     * future, the data future may be not ready.
     *
     * <p>The ready future is used for {@link ChannelStateWriteRequestExecutorImpl}, executor will
     * process ready requests first to avoid deadlock.
     */
    public CompletableFuture<?> getReadyFuture() {
        return AvailabilityProvider.AVAILABLE;
    }

    @Override
    public String toString() {
        return name
                + " {jobVertexID="
                + jobVertexID
                + ", subtaskIndex="
                + subtaskIndex
                + ", checkpointId="
                + checkpointId
                + '}';
    }

    abstract void cancel(Throwable cause) throws Exception;

    static CheckpointInProgressRequest completeInput(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId) {
        return new CheckpointInProgressRequest(
                "completeInput",
                jobVertexID,
                subtaskIndex,
                checkpointId,
                writer -> writer.completeInput(jobVertexID, subtaskIndex));
    }

    static CheckpointInProgressRequest completeOutput(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId) {
        return new CheckpointInProgressRequest(
                "completeOutput",
                jobVertexID,
                subtaskIndex,
                checkpointId,
                writer -> writer.completeOutput(jobVertexID, subtaskIndex));
    }

    static ChannelStateWriteRequest write(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            InputChannelInfo info,
            CloseableIterator<Buffer> iterator) {
        return buildWriteRequest(
                jobVertexID,
                subtaskIndex,
                checkpointId,
                "writeInput",
                iterator,
                (writer, buffer) -> writer.writeInput(jobVertexID, subtaskIndex, info, buffer));
    }

    static ChannelStateWriteRequest write(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ResultSubpartitionInfo info,
            Buffer... buffers) {
        return buildWriteRequest(
                jobVertexID,
                subtaskIndex,
                checkpointId,
                "writeOutput",
                ofElements(Buffer::recycleBuffer, buffers),
                (writer, buffer) -> writer.writeOutput(jobVertexID, subtaskIndex, info, buffer));
    }

    static ChannelStateWriteRequest write(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ResultSubpartitionInfo info,
            CompletableFuture<List<Buffer>> dataFuture) {
        return buildFutureWriteRequest(
                jobVertexID,
                subtaskIndex,
                checkpointId,
                "writeOutputFuture",
                dataFuture,
                (writer, buffer) -> writer.writeOutput(jobVertexID, subtaskIndex, info, buffer));
    }

    static ChannelStateWriteRequest buildFutureWriteRequest(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            String name,
            CompletableFuture<List<Buffer>> dataFuture,
            BiConsumer<ChannelStateCheckpointWriter, Buffer> bufferConsumer) {
        return new CheckpointInProgressRequest(
                name,
                jobVertexID,
                subtaskIndex,
                checkpointId,
                writer -> {
                    checkState(
                            dataFuture.isDone(), "It should be executed when dataFuture is done.");
                    List<Buffer> buffers;
                    try {
                        buffers = dataFuture.get();
                    } catch (ExecutionException e) {
                        // If dataFuture fails, fail only the single related writer
                        writer.fail(jobVertexID, subtaskIndex, e);
                        return;
                    }
                    for (Buffer buffer : buffers) {
                        checkBufferIsBuffer(buffer);
                        bufferConsumer.accept(writer, buffer);
                    }
                },
                throwable ->
                        dataFuture.thenAccept(
                                buffers -> {
                                    try {
                                        CloseableIterator.fromList(buffers, Buffer::recycleBuffer)
                                                .close();
                                    } catch (Exception e) {
                                        LOG.error(
                                                "Failed to recycle the output buffer of channel state.",
                                                e);
                                    }
                                }),
                dataFuture);
    }

    static ChannelStateWriteRequest buildWriteRequest(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            String name,
            CloseableIterator<Buffer> iterator,
            BiConsumer<ChannelStateCheckpointWriter, Buffer> bufferConsumer) {
        return new CheckpointInProgressRequest(
                name,
                jobVertexID,
                subtaskIndex,
                checkpointId,
                writer -> {
                    while (iterator.hasNext()) {
                        Buffer buffer = iterator.next();
                        checkBufferIsBuffer(buffer);
                        bufferConsumer.accept(writer, buffer);
                    }
                },
                throwable -> iterator.close());
    }

    static void checkBufferIsBuffer(Buffer buffer) {
        try {
            checkArgument(buffer.isBuffer());
        } catch (Exception e) {
            buffer.recycleBuffer();
            throw e;
        }
    }

    static ChannelStateWriteRequest start(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriteResult targetResult,
            CheckpointStorageLocationReference locationReference) {
        return new CheckpointStartRequest(
                jobVertexID, subtaskIndex, checkpointId, targetResult, locationReference);
    }

    static ChannelStateWriteRequest abort(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId, Throwable cause) {
        return new CheckpointAbortRequest(jobVertexID, subtaskIndex, checkpointId, cause);
    }

    static ChannelStateWriteRequest registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        return new SubtaskRegisterRequest(jobVertexID, subtaskIndex);
    }

    static ChannelStateWriteRequest releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        return new SubtaskReleaseRequest(jobVertexID, subtaskIndex);
    }

    static ThrowingConsumer<Throwable, Exception> recycle(Buffer[] flinkBuffers) {
        return unused -> {
            for (Buffer b : flinkBuffers) {
                b.recycleBuffer();
            }
        };
    }
}

final class CheckpointStartRequest extends ChannelStateWriteRequest {

    private final ChannelStateWriteResult targetResult;
    private final CheckpointStorageLocationReference locationReference;

    CheckpointStartRequest(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriteResult targetResult,
            CheckpointStorageLocationReference locationReference) {
        super(jobVertexID, subtaskIndex, checkpointId, "Start");
        this.targetResult = checkNotNull(targetResult);
        this.locationReference = checkNotNull(locationReference);
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
}

enum CheckpointInProgressRequestState {
    NEW,
    EXECUTING,
    COMPLETED,
    FAILED,
    CANCELLED
}

final class CheckpointInProgressRequest extends ChannelStateWriteRequest {
    private final ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action;
    private final ThrowingConsumer<Throwable, Exception> discardAction;
    private final AtomicReference<CheckpointInProgressRequestState> state =
            new AtomicReference<>(NEW);
    @Nullable private final CompletableFuture<?> readyFuture;

    CheckpointInProgressRequest(
            String name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action) {
        this(name, jobVertexID, subtaskIndex, checkpointId, action, unused -> {});
    }

    CheckpointInProgressRequest(
            String name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action,
            ThrowingConsumer<Throwable, Exception> discardAction) {
        this(name, jobVertexID, subtaskIndex, checkpointId, action, discardAction, null);
    }

    CheckpointInProgressRequest(
            String name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ThrowingConsumer<ChannelStateCheckpointWriter, Exception> action,
            ThrowingConsumer<Throwable, Exception> discardAction,
            @Nullable CompletableFuture<?> readyFuture) {
        super(jobVertexID, subtaskIndex, checkpointId, name);
        this.action = checkNotNull(action);
        this.discardAction = checkNotNull(discardAction);
        this.readyFuture = readyFuture;
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

    @Override
    public CompletableFuture<?> getReadyFuture() {
        if (readyFuture != null) {
            return readyFuture;
        }
        return super.getReadyFuture();
    }
}

final class CheckpointAbortRequest extends ChannelStateWriteRequest {

    private final Throwable throwable;

    public CheckpointAbortRequest(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId, Throwable throwable) {
        super(jobVertexID, subtaskIndex, checkpointId, "Abort");
        this.throwable = throwable;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public void cancel(Throwable cause) throws Exception {}

    @Override
    public String toString() {
        return String.format("%s, cause : %s.", super.toString(), throwable);
    }
}

final class SubtaskRegisterRequest extends ChannelStateWriteRequest {

    public SubtaskRegisterRequest(JobVertexID jobVertexID, int subtaskIndex) {
        super(jobVertexID, subtaskIndex, 0, "Register");
    }

    @Override
    public void cancel(Throwable cause) throws Exception {}
}

final class SubtaskReleaseRequest extends ChannelStateWriteRequest {

    public SubtaskReleaseRequest(JobVertexID jobVertexID, int subtaskIndex) {
        super(jobVertexID, subtaskIndex, 0, "Release");
    }

    @Override
    public void cancel(Throwable cause) throws Exception {}
}
