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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeInput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.completeOutput;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequest.write;

/**
 * {@link ChannelStateWriter} implemented using {@link CheckpointStateOutputStream
 * CheckpointStateOutputStreams}. Internally, it has by default
 *
 * <ul>
 *   <li>one stream per checkpoint; having multiple streams would mean more files written and more
 *       connections opened (and more latency on restore)
 *   <li>one thread; having multiple threads means more connections, couples with the implementation
 *       and increases complexity
 * </ul>
 *
 * <p>Thread-safety: this class is thread-safe when used with a thread-safe {@link
 * ChannelStateWriteRequestExecutor executor} (e.g. default {@link
 * ChannelStateWriteRequestExecutorImpl}.
 */
@Internal
@ThreadSafe
public class ChannelStateWriterImpl implements ChannelStateWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateWriterImpl.class);
    private static final int DEFAULT_MAX_CHECKPOINTS =
            1000; // includes max-concurrent-checkpoints + checkpoints to be aborted (scheduled via
    // mailbox)

    private final JobVertexID jobVertexID;

    private final int subtaskIndex;

    private final String taskName;

    private final ChannelStateWriteRequestExecutor executor;
    private final ConcurrentMap<Long, ChannelStateWriteResult> results;
    private final int maxCheckpoints;

    private final AtomicBoolean wasClosed = new AtomicBoolean(false);

    /**
     * Creates a {@link ChannelStateWriterImpl} with {@link #DEFAULT_MAX_CHECKPOINTS} as {@link
     * #maxCheckpoints}.
     */
    public ChannelStateWriterImpl(
            JobVertexID jobVertexID,
            String taskName,
            int subtaskIndex,
            CheckpointStorage checkpointStorage,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory,
            int maxSubtasksPerChannelStateFile) {
        this(
                jobVertexID,
                taskName,
                subtaskIndex,
                checkpointStorage,
                DEFAULT_MAX_CHECKPOINTS,
                channelStateExecutorFactory,
                maxSubtasksPerChannelStateFile);
    }

    /**
     * Creates a {@link ChannelStateWriterImpl} with {@link ChannelStateSerializerImpl default}
     * {@link ChannelStateSerializer}, and a {@link ChannelStateWriteRequestExecutorImpl}.
     *
     * @param taskName
     * @param checkpointStorage a factory to obtain output stream factory for a given checkpoint
     * @param maxCheckpoints maximum number of checkpoints to be written currently or finished but
     *     not taken yet.
     */
    ChannelStateWriterImpl(
            JobVertexID jobVertexID,
            String taskName,
            int subtaskIndex,
            CheckpointStorage checkpointStorage,
            int maxCheckpoints,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory,
            int maxSubtasksPerChannelStateFile) {
        this(
                jobVertexID,
                taskName,
                subtaskIndex,
                new ConcurrentHashMap<>(maxCheckpoints),
                channelStateExecutorFactory.getOrCreateExecutor(
                        jobVertexID,
                        subtaskIndex,
                        checkpointStorage,
                        maxSubtasksPerChannelStateFile),
                maxCheckpoints);
    }

    ChannelStateWriterImpl(
            JobVertexID jobVertexID,
            String taskName,
            int subtaskIndex,
            ConcurrentMap<Long, ChannelStateWriteResult> results,
            ChannelStateWriteRequestExecutor executor,
            int maxCheckpoints) {
        this.jobVertexID = jobVertexID;
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.results = results;
        this.maxCheckpoints = maxCheckpoints;
        this.executor = executor;
    }

    @Override
    public void start(long checkpointId, CheckpointOptions checkpointOptions) {
        LOG.debug("{} starting checkpoint {} ({})", taskName, checkpointId, checkpointOptions);
        ChannelStateWriteResult result = new ChannelStateWriteResult();
        ChannelStateWriteResult put =
                results.computeIfAbsent(
                        checkpointId,
                        id -> {
                            Preconditions.checkState(
                                    results.size() < maxCheckpoints,
                                    String.format(
                                            "%s can't start %d, results.size() > maxCheckpoints: %d > %d",
                                            taskName,
                                            checkpointId,
                                            results.size(),
                                            maxCheckpoints));
                            enqueue(
                                    new CheckpointStartRequest(
                                            jobVertexID,
                                            subtaskIndex,
                                            checkpointId,
                                            result,
                                            checkpointOptions.getTargetLocation()),
                                    false);
                            return result;
                        });
        Preconditions.checkArgument(
                put == result,
                taskName + " result future already present for checkpoint " + checkpointId);
    }

    @Override
    public void addInputData(
            long checkpointId,
            InputChannelInfo info,
            int startSeqNum,
            CloseableIterator<Buffer> iterator) {
        LOG.trace(
                "{} adding input data, checkpoint {}, channel: {}, startSeqNum: {}",
                taskName,
                checkpointId,
                info,
                startSeqNum);
        enqueue(write(jobVertexID, subtaskIndex, checkpointId, info, iterator), false);
    }

    @Override
    public void addOutputData(
            long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
        LOG.trace(
                "{} adding output data, checkpoint {}, channel: {}, startSeqNum: {}, num buffers: {}",
                taskName,
                checkpointId,
                info,
                startSeqNum,
                data == null ? 0 : data.length);
        enqueue(write(jobVertexID, subtaskIndex, checkpointId, info, data), false);
    }

    @Override
    public void addOutputDataFuture(
            long checkpointId,
            ResultSubpartitionInfo info,
            int startSeqNum,
            CompletableFuture<List<Buffer>> dataFuture)
            throws IllegalArgumentException {
        LOG.trace(
                "{} adding output data future, checkpoint {}, channel: {}, startSeqNum: {}",
                taskName,
                checkpointId,
                info,
                startSeqNum);
        enqueue(write(jobVertexID, subtaskIndex, checkpointId, info, dataFuture), false);
    }

    @Override
    public void finishInput(long checkpointId) {
        LOG.debug("{} finishing input data, checkpoint {}", taskName, checkpointId);
        enqueue(completeInput(jobVertexID, subtaskIndex, checkpointId), false);
    }

    @Override
    public void finishOutput(long checkpointId) {
        LOG.debug("{} finishing output data, checkpoint {}", taskName, checkpointId);
        enqueue(completeOutput(jobVertexID, subtaskIndex, checkpointId), false);
    }

    @Override
    public void abort(long checkpointId, Throwable cause, boolean cleanup) {
        LOG.debug("{} aborting, checkpoint {}", taskName, checkpointId);
        enqueue(
                ChannelStateWriteRequest.abort(jobVertexID, subtaskIndex, checkpointId, cause),
                true); // abort already started
        enqueue(
                ChannelStateWriteRequest.abort(jobVertexID, subtaskIndex, checkpointId, cause),
                false); // abort enqueued but not started
        if (cleanup) {
            results.remove(checkpointId);
        }
    }

    @Override
    public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
        LOG.debug("{} requested write result, checkpoint {}", taskName, checkpointId);
        ChannelStateWriteResult result = results.remove(checkpointId);
        Preconditions.checkArgument(
                result != null,
                taskName + " channel state write result not found for checkpoint " + checkpointId);
        return result;
    }

    // just for test
    @VisibleForTesting
    public ChannelStateWriteResult getWriteResult(long checkpointId) {
        return results.get(checkpointId);
    }

    @Override
    public void close() throws IOException {
        if (wasClosed.compareAndSet(false, true)) {
            LOG.debug("close, dropping checkpoints {}", results.keySet());
            results.clear();
            enqueue(ChannelStateWriteRequest.releaseSubtask(jobVertexID, subtaskIndex), false);
            executor.releaseSubtask(jobVertexID, subtaskIndex);
        }
    }

    private void enqueue(ChannelStateWriteRequest request, boolean atTheFront) {
        // state check and previous errors check are performed inside the worker
        try {
            if (atTheFront) {
                executor.submitPriority(request);
            } else {
                executor.submit(request);
            }
        } catch (Exception e) {
            RuntimeException wrapped = new RuntimeException("unable to send request to worker", e);
            try {
                request.cancel(e);
            } catch (Exception cancelException) {
                wrapped.addSuppressed(cancelException);
            }
            throw wrapped;
        }
    }
}
