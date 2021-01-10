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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.util.ExceptionUtils.rethrow;

/**
 * A no op implementation that performs basic checks of the contract, but does not actually write
 * any data.
 */
public class MockChannelStateWriter implements ChannelStateWriter {
    private volatile ChannelStateWriteResult channelStateWriteResult =
            ChannelStateWriteResult.EMPTY;
    private volatile long startedCheckpointId = -1;
    private final boolean autoComplete;

    public MockChannelStateWriter() {
        this(true);
    }

    public MockChannelStateWriter(boolean autoComplete) {
        this.autoComplete = autoComplete;
    }

    @Override
    public void start(long checkpointId, CheckpointOptions checkpointOptions) {
        if (checkpointId == startedCheckpointId) {
            throw new IllegalStateException("Already started " + checkpointId);
        } else if (checkpointId < startedCheckpointId) {
            throw new IllegalArgumentException(
                    "Expected a larger checkpoint id than "
                            + startedCheckpointId
                            + " but "
                            + "got "
                            + checkpointId);
        }
        startedCheckpointId = checkpointId;
        channelStateWriteResult = new ChannelStateWriteResult();
    }

    @Override
    public void addInputData(
            long checkpointId,
            InputChannelInfo info,
            int startSeqNum,
            CloseableIterator<Buffer> iterator) {
        checkCheckpointId(checkpointId);
        try {
            iterator.close();
        } catch (Exception e) {
            rethrow(e);
        }
    }

    @Override
    public void addOutputData(
            long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {
        checkCheckpointId(checkpointId);
        for (final Buffer buffer : data) {
            buffer.recycleBuffer();
        }
    }

    @Override
    public void finishInput(long checkpointId) {
        checkCheckpointId(checkpointId);
        if (autoComplete) {
            completeInput();
        }
    }

    public void completeInput() {
        channelStateWriteResult.getInputChannelStateHandles().complete(null);
    }

    @Override
    public void finishOutput(long checkpointId) {
        checkCheckpointId(checkpointId);
        if (autoComplete) {
            completeOutput();
        }
    }

    public void completeOutput() {
        channelStateWriteResult.getResultSubpartitionStateHandles().complete(null);
    }

    protected void checkCheckpointId(long checkpointId) {
        if (checkpointId != startedCheckpointId) {
            throw new IllegalStateException(
                    "Need to have recently called #start with "
                            + checkpointId
                            + " but "
                            + "currently started checkpoint id is "
                            + startedCheckpointId);
        }
    }

    @Override
    public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
        return channelStateWriteResult;
    }

    @Override
    public void abort(long checkpointId, Throwable cause, boolean cleanup) {
        checkCheckpointId(checkpointId);
        channelStateWriteResult.getInputChannelStateHandles().cancel(false);
        channelStateWriteResult.getResultSubpartitionStateHandles().cancel(false);
    }

    @Override
    public void close() {
        channelStateWriteResult.getInputChannelStateHandles().cancel(false);
        channelStateWriteResult.getResultSubpartitionStateHandles().cancel(false);
    }
}
