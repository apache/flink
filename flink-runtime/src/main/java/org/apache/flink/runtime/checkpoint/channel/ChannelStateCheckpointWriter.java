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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.AbstractChannelStateHandle.StateContentMetaInfo;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHANNEL_STATE_SHARED_STREAM_EXCEPTION;
import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Writes channel state for multiple subtasks of the same checkpoint. */
@NotThreadSafe
class ChannelStateCheckpointWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateCheckpointWriter.class);

    private final DataOutputStream dataStream;
    private final CheckpointStateOutputStream checkpointStream;

    /**
     * Indicates whether the current checkpoints of all subtasks have exception. If it's not null,
     * the checkpoint will fail.
     */
    private Throwable throwable;

    private final ChannelStateSerializer serializer;
    private final long checkpointId;
    private final RunnableWithException onComplete;

    // Subtasks that have not yet register writer result.
    private final Set<SubtaskID> subtasksToRegister;

    private final Map<SubtaskID, ChannelStatePendingResult> pendingResults = new HashMap<>();

    ChannelStateCheckpointWriter(
            Set<SubtaskID> subtasks,
            long checkpointId,
            CheckpointStreamFactory streamFactory,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete)
            throws Exception {
        this(
                subtasks,
                checkpointId,
                streamFactory.createCheckpointStateOutputStream(EXCLUSIVE),
                serializer,
                onComplete);
    }

    @VisibleForTesting
    ChannelStateCheckpointWriter(
            Set<SubtaskID> subtasks,
            long checkpointId,
            CheckpointStateOutputStream stream,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete) {
        this(subtasks, checkpointId, serializer, onComplete, stream, new DataOutputStream(stream));
    }

    @VisibleForTesting
    ChannelStateCheckpointWriter(
            Set<SubtaskID> subtasks,
            long checkpointId,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete,
            CheckpointStateOutputStream checkpointStateOutputStream,
            DataOutputStream dataStream) {
        checkArgument(!subtasks.isEmpty(), "The subtasks cannot be empty.");
        this.subtasksToRegister = new HashSet<>(subtasks);
        this.checkpointId = checkpointId;
        this.checkpointStream = checkNotNull(checkpointStateOutputStream);
        this.serializer = checkNotNull(serializer);
        this.dataStream = checkNotNull(dataStream);
        this.onComplete = checkNotNull(onComplete);
        runWithChecks(() -> serializer.writeHeader(dataStream));
    }

    void registerSubtaskResult(
            SubtaskID subtaskID, ChannelStateWriter.ChannelStateWriteResult result) {
        // The writer shouldn't register any subtask after writer has exception or is done,
        checkState(!isDone(), "The write is done.");
        Preconditions.checkState(
                !pendingResults.containsKey(subtaskID),
                "The subtask %s has already been register before.",
                subtaskID);
        subtasksToRegister.remove(subtaskID);

        ChannelStatePendingResult pendingResult =
                new ChannelStatePendingResult(
                        subtaskID.getSubtaskIndex(), checkpointId, result, serializer);
        pendingResults.put(subtaskID, pendingResult);
    }

    void releaseSubtask(SubtaskID subtaskID) throws Exception {
        if (subtasksToRegister.remove(subtaskID)) {
            // If all checkpoint of other subtasks of this writer are completed, and
            // writer is waiting for the last subtask. After the last subtask is finished,
            // the writer should be completed.
            tryFinishResult();
        }
    }

    void writeInput(
            JobVertexID jobVertexID, int subtaskIndex, InputChannelInfo info, Buffer buffer) {
        try {
            if (isDone()) {
                return;
            }
            ChannelStatePendingResult pendingResult =
                    getChannelStatePendingResult(jobVertexID, subtaskIndex);
            write(
                    pendingResult.getInputChannelOffsets(),
                    info,
                    buffer,
                    !pendingResult.isAllInputsReceived(),
                    "ChannelStateCheckpointWriter#writeInput");
        } finally {
            buffer.recycleBuffer();
        }
    }

    void writeOutput(
            JobVertexID jobVertexID, int subtaskIndex, ResultSubpartitionInfo info, Buffer buffer) {
        try {
            if (isDone()) {
                return;
            }
            ChannelStatePendingResult pendingResult =
                    getChannelStatePendingResult(jobVertexID, subtaskIndex);
            write(
                    pendingResult.getResultSubpartitionOffsets(),
                    info,
                    buffer,
                    !pendingResult.isAllOutputsReceived(),
                    "ChannelStateCheckpointWriter#writeOutput");
        } finally {
            buffer.recycleBuffer();
        }
    }

    private <K> void write(
            Map<K, StateContentMetaInfo> offsets,
            K key,
            Buffer buffer,
            boolean precondition,
            String action) {
        runWithChecks(
                () -> {
                    checkState(precondition);
                    long offset = checkpointStream.getPos();
                    try (AutoCloseable ignored = NetworkActionsLogger.measureIO(action, buffer)) {
                        serializer.writeData(dataStream, buffer);
                    }
                    long size = checkpointStream.getPos() - offset;
                    offsets.computeIfAbsent(key, unused -> new StateContentMetaInfo())
                            .withDataAdded(offset, size);
                    NetworkActionsLogger.tracePersist(action, buffer, key, checkpointId);
                });
    }

    void completeInput(JobVertexID jobVertexID, int subtaskIndex) throws Exception {
        if (isDone()) {
            return;
        }
        getChannelStatePendingResult(jobVertexID, subtaskIndex).completeInput();
        tryFinishResult();
    }

    void completeOutput(JobVertexID jobVertexID, int subtaskIndex) throws Exception {
        if (isDone()) {
            return;
        }
        getChannelStatePendingResult(jobVertexID, subtaskIndex).completeOutput();
        tryFinishResult();
    }

    public void tryFinishResult() throws Exception {
        if (!subtasksToRegister.isEmpty()) {
            // Some subtasks are not registered yet
            return;
        }
        for (ChannelStatePendingResult result : pendingResults.values()) {
            if (result.isAllInputsReceived() && result.isAllOutputsReceived()) {
                continue;
            }
            // Some subtasks did not receive all buffers
            return;
        }

        if (isDone()) {
            // likely after abort - only need to set the flag run onComplete callback
            doComplete(onComplete);
        } else {
            runWithChecks(() -> doComplete(onComplete, this::finishWriteAndResult));
        }
    }

    private void finishWriteAndResult() throws IOException {
        StreamStateHandle stateHandle = null;
        if (checkpointStream.getPos() == serializer.getHeaderLength()) {
            dataStream.close();
        } else {
            dataStream.flush();
            stateHandle = checkpointStream.closeAndGetHandle();
        }
        for (ChannelStatePendingResult result : pendingResults.values()) {
            result.finishResult(stateHandle);
        }
    }

    private void doComplete(RunnableWithException... callbacks) throws Exception {
        for (RunnableWithException callback : callbacks) {
            callback.run();
        }
    }

    public boolean isDone() {
        if (throwable != null) {
            return true;
        }
        for (ChannelStatePendingResult result : pendingResults.values()) {
            if (result.isDone()) {
                return true;
            }
        }
        return false;
    }

    private void runWithChecks(RunnableWithException r) {
        try {
            checkState(!isDone(), "results are already completed", pendingResults.values());
            r.run();
        } catch (Exception e) {
            fail(e);
            if (!findThrowable(e, IOException.class).isPresent()) {
                rethrow(e);
            }
        }
    }

    /**
     * The throwable is just used for specific subtask that triggered the failure. Other subtasks
     * should fail by {@link CHANNEL_STATE_SHARED_STREAM_EXCEPTION}.
     */
    public void fail(JobVertexID jobVertexID, int subtaskIndex, Throwable throwable) {
        if (isDone()) {
            return;
        }
        this.throwable = throwable;

        ChannelStatePendingResult result =
                pendingResults.get(SubtaskID.of(jobVertexID, subtaskIndex));
        if (result != null) {
            result.fail(throwable);
        }
        failResultAndCloseStream(
                new CheckpointException(CHANNEL_STATE_SHARED_STREAM_EXCEPTION, throwable));
    }

    public void fail(Throwable throwable) {
        if (isDone()) {
            return;
        }
        this.throwable = throwable;

        failResultAndCloseStream(throwable);
    }

    public void failResultAndCloseStream(Throwable e) {
        for (ChannelStatePendingResult result : pendingResults.values()) {
            result.fail(e);
        }
        try {
            checkpointStream.close();
        } catch (Exception closeException) {
            String message = "Unable to close checkpointStream after a failure";
            if (findThrowable(closeException, IOException.class).isPresent()) {
                LOG.warn(message, closeException);
            } else {
                throw new RuntimeException(message, closeException);
            }
        }
    }

    @Nonnull
    private ChannelStatePendingResult getChannelStatePendingResult(
            JobVertexID jobVertexID, int subtaskIndex) {
        SubtaskID subtaskID = SubtaskID.of(jobVertexID, subtaskIndex);
        ChannelStatePendingResult pendingResult = pendingResults.get(subtaskID);
        checkNotNull(pendingResult, "The subtask[%s] is not registered yet", subtaskID);
        return pendingResult;
    }
}

/** A identification for subtask. */
class SubtaskID {

    private final JobVertexID jobVertexID;
    private final int subtaskIndex;

    private SubtaskID(JobVertexID jobVertexID, int subtaskIndex) {
        this.jobVertexID = jobVertexID;
        this.subtaskIndex = subtaskIndex;
    }

    public JobVertexID getJobVertexID() {
        return jobVertexID;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public static SubtaskID of(JobVertexID jobVertexID, int subtaskIndex) {
        return new SubtaskID(jobVertexID, subtaskIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubtaskID subtaskID = (SubtaskID) o;
        return subtaskIndex == subtaskID.subtaskIndex
                && Objects.equals(jobVertexID, subtaskID.jobVertexID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobVertexID, subtaskIndex);
    }

    @Override
    public String toString() {
        return "SubtaskID{" + "jobVertexID=" + jobVertexID + ", subtaskIndex=" + subtaskIndex + '}';
    }
}
