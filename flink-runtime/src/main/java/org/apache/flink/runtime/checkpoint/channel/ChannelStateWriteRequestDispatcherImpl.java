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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHANNEL_STATE_SHARED_STREAM_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Maintains a set of {@link ChannelStateCheckpointWriter writers} per checkpoint and translates
 * incoming {@link ChannelStateWriteRequest requests} to their corresponding methods.
 */
final class ChannelStateWriteRequestDispatcherImpl implements ChannelStateWriteRequestDispatcher {
    private static final Logger LOG =
            LoggerFactory.getLogger(ChannelStateWriteRequestDispatcherImpl.class);

    private final CheckpointStorage checkpointStorage;

    private final JobID jobID;

    private final ChannelStateSerializer serializer;

    private final Set<SubtaskID> registeredSubtasks;

    private CheckpointStorageWorkerView streamFactoryResolver;

    /**
     * It is the checkpointId corresponding to writer. And It should be always update with {@link
     * #writer}.
     */
    private long ongoingCheckpointId;

    /**
     * The checkpoint that checkpointId is less than or equal to maxAbortedCheckpointId should be
     * aborted.
     */
    private long maxAbortedCheckpointId;

    /** The aborted subtask of the maxAbortedCheckpointId. */
    private SubtaskID abortedSubtaskID;

    /** The aborted cause of the maxAbortedCheckpointId. */
    private Throwable abortedCause;

    /**
     * The channelState writer of ongoing checkpointId, it can be null when the writer is finished.
     */
    private ChannelStateCheckpointWriter writer;

    ChannelStateWriteRequestDispatcherImpl(
            CheckpointStorage checkpointStorage, JobID jobID, ChannelStateSerializer serializer) {
        this.checkpointStorage = checkNotNull(checkpointStorage);
        this.jobID = jobID;
        this.serializer = checkNotNull(serializer);
        this.registeredSubtasks = new HashSet<>();
        this.ongoingCheckpointId = -1;
        this.maxAbortedCheckpointId = -1;
    }

    @Override
    public void dispatch(ChannelStateWriteRequest request) throws Exception {
        LOG.trace("process {}", request);
        try {
            dispatchInternal(request);
        } catch (Exception e) {
            try {
                request.cancel(e);
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    private void dispatchInternal(ChannelStateWriteRequest request) throws Exception {
        if (request instanceof SubtaskRegisterRequest) {
            SubtaskRegisterRequest req = (SubtaskRegisterRequest) request;
            SubtaskID subtaskID = SubtaskID.of(req.getJobVertexID(), req.getSubtaskIndex());
            registeredSubtasks.add(subtaskID);
            return;
        } else if (request instanceof SubtaskReleaseRequest) {
            SubtaskReleaseRequest req = (SubtaskReleaseRequest) request;
            SubtaskID subtaskID = SubtaskID.of(req.getJobVertexID(), req.getSubtaskIndex());
            registeredSubtasks.remove(subtaskID);
            if (writer == null) {
                return;
            }
            writer.releaseSubtask(subtaskID);
            return;
        }

        if (isAbortedCheckpoint(request.getCheckpointId())) {
            handleAbortedRequest(request);
        } else if (request instanceof CheckpointStartRequest) {
            handleCheckpointStartRequest(request);
        } else if (request instanceof CheckpointInProgressRequest) {
            handleCheckpointInProgressRequest((CheckpointInProgressRequest) request);
        } else if (request instanceof CheckpointAbortRequest) {
            handleCheckpointAbortRequest(request);
        } else {
            throw new IllegalArgumentException("unknown request type: " + request);
        }
    }

    private void handleAbortedRequest(ChannelStateWriteRequest request) throws Exception {
        if (request.getCheckpointId() != maxAbortedCheckpointId) {
            request.cancel(new CheckpointException(CHECKPOINT_DECLINED_SUBSUMED));
            return;
        }

        SubtaskID requestSubtask =
                SubtaskID.of(request.getJobVertexID(), request.getSubtaskIndex());
        if (requestSubtask.equals(abortedSubtaskID)) {
            request.cancel(abortedCause);
        } else {
            request.cancel(
                    new CheckpointException(CHANNEL_STATE_SHARED_STREAM_EXCEPTION, abortedCause));
        }
    }

    private void handleCheckpointStartRequest(ChannelStateWriteRequest request) throws Exception {
        checkState(
                request.getCheckpointId() >= ongoingCheckpointId,
                String.format(
                        "Checkpoint must be incremented, ongoingCheckpointId is %s, but the request is %s.",
                        ongoingCheckpointId, request));
        if (request.getCheckpointId() > ongoingCheckpointId) {
            // Clear the previous writer.
            failAndClearWriter(new CheckpointException(CHECKPOINT_DECLINED_SUBSUMED));
        }
        CheckpointStartRequest req = (CheckpointStartRequest) request;
        // The writer may not be null due to other subtask may have built writer for
        // ongoingCheckpointId when multiple subtasks share channel state file.
        if (writer == null) {
            this.writer = buildWriter(req);
            this.ongoingCheckpointId = request.getCheckpointId();
        }
        writer.registerSubtaskResult(
                SubtaskID.of(req.getJobVertexID(), req.getSubtaskIndex()), req.getTargetResult());
    }

    private void handleCheckpointInProgressRequest(CheckpointInProgressRequest req)
            throws Exception {
        checkArgument(
                ongoingCheckpointId == req.getCheckpointId() && writer != null,
                "writer not found while processing request: " + req);
        req.execute(writer);
    }

    private void handleCheckpointAbortRequest(ChannelStateWriteRequest request) {
        CheckpointAbortRequest req = (CheckpointAbortRequest) request;
        if (request.getCheckpointId() > maxAbortedCheckpointId) {
            this.maxAbortedCheckpointId = req.getCheckpointId();
            this.abortedCause = req.getThrowable();
            this.abortedSubtaskID = SubtaskID.of(req.getJobVertexID(), req.getSubtaskIndex());
        }

        if (req.getCheckpointId() == ongoingCheckpointId) {
            failAndClearWriter(req.getJobVertexID(), req.getSubtaskIndex(), req.getThrowable());
        } else if (request.getCheckpointId() > ongoingCheckpointId) {
            failAndClearWriter(new CheckpointException(CHECKPOINT_DECLINED_SUBSUMED));
        }
    }

    private boolean isAbortedCheckpoint(long checkpointId) {
        return checkpointId < ongoingCheckpointId || checkpointId <= maxAbortedCheckpointId;
    }

    private void failAndClearWriter(Throwable e) {
        if (writer == null) {
            return;
        }
        writer.fail(e);
        writer = null;
    }

    private void failAndClearWriter(
            JobVertexID jobVertexID, int subtaskIndex, Throwable throwable) {
        if (writer == null) {
            return;
        }
        writer.fail(jobVertexID, subtaskIndex, throwable);
        writer = null;
    }

    private ChannelStateCheckpointWriter buildWriter(CheckpointStartRequest request)
            throws Exception {
        return new ChannelStateCheckpointWriter(
                registeredSubtasks,
                request.getCheckpointId(),
                getStreamFactoryResolver()
                        .resolveCheckpointStorageLocation(
                                request.getCheckpointId(), request.getLocationReference()),
                serializer,
                () -> {
                    checkState(
                            request.getCheckpointId() == ongoingCheckpointId,
                            "The ongoingCheckpointId[%s] was changed when clear writer of checkpoint[%s], it might be a bug.",
                            ongoingCheckpointId,
                            request.getCheckpointId());
                    this.writer = null;
                });
    }

    @Override
    public void fail(Throwable cause) {
        if (writer == null) {
            return;
        }
        try {
            writer.fail(cause);
        } catch (Exception ex) {
            LOG.warn("unable to fail write channel state writer", cause);
        }
        writer = null;
    }

    CheckpointStorageWorkerView getStreamFactoryResolver() throws IOException {
        if (streamFactoryResolver == null) {
            streamFactoryResolver = checkpointStorage.createCheckpointStorage(jobID);
        }
        return streamFactoryResolver;
    }
}
