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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final CheckpointStorageWorkerView streamFactoryResolver;
    private final ChannelStateSerializer serializer;
    private final int subtaskIndex;
    private final String taskName;

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

    /** The aborted cause of the maxAbortedCheckpointId. */
    private Throwable abortedCause;

    /**
     * The channelState writer of ongoing checkpointId, it can be null when the writer is finished.
     */
    private ChannelStateCheckpointWriter writer;

    ChannelStateWriteRequestDispatcherImpl(
            String taskName,
            int subtaskIndex,
            CheckpointStorageWorkerView streamFactoryResolver,
            ChannelStateSerializer serializer) {
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.streamFactoryResolver = checkNotNull(streamFactoryResolver);
        this.serializer = checkNotNull(serializer);
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
        if (isAbortedCheckpoint(request.getCheckpointId())) {
            if (request.getCheckpointId() == maxAbortedCheckpointId) {
                request.cancel(abortedCause);
            } else {
                request.cancel(new CheckpointException(CHECKPOINT_DECLINED_SUBSUMED));
            }
            return;
        }

        if (request instanceof CheckpointStartRequest) {
            checkState(
                    request.getCheckpointId() > ongoingCheckpointId,
                    String.format(
                            "Checkpoint must be incremented, ongoingCheckpointId is %s, but the request is %s.",
                            ongoingCheckpointId, request));
            failAndClearWriter(
                    new IllegalStateException(
                            String.format(
                                    "Task[name=%s, subtaskIndex=%s] has uncompleted channelState writer of checkpointId=%s, "
                                            + "but it received a new checkpoint start request of checkpointId=%s, it maybe "
                                            + "a bug due to currently not supported concurrent unaligned checkpoint.",
                                    taskName,
                                    subtaskIndex,
                                    ongoingCheckpointId,
                                    request.getCheckpointId())));
            this.writer = buildWriter((CheckpointStartRequest) request);
            this.ongoingCheckpointId = request.getCheckpointId();
        } else if (request instanceof CheckpointInProgressRequest) {
            CheckpointInProgressRequest req = (CheckpointInProgressRequest) request;
            checkArgument(
                    ongoingCheckpointId == req.getCheckpointId() && writer != null,
                    "writer not found while processing request: " + req);
            req.execute(writer);
        } else if (request instanceof CheckpointAbortRequest) {
            CheckpointAbortRequest req = (CheckpointAbortRequest) request;
            if (request.getCheckpointId() > maxAbortedCheckpointId) {
                this.maxAbortedCheckpointId = req.getCheckpointId();
                this.abortedCause = req.getThrowable();
            }

            if (req.getCheckpointId() == ongoingCheckpointId) {
                failAndClearWriter(req.getThrowable());
            } else if (request.getCheckpointId() > ongoingCheckpointId) {
                failAndClearWriter(new CheckpointException(CHECKPOINT_DECLINED_SUBSUMED));
            }
        } else {
            throw new IllegalArgumentException("unknown request type: " + request);
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

    private ChannelStateCheckpointWriter buildWriter(CheckpointStartRequest request)
            throws Exception {
        return new ChannelStateCheckpointWriter(
                taskName,
                subtaskIndex,
                request,
                streamFactoryResolver.resolveCheckpointStorageLocation(
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
}
