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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.AbstractChannelStateHandle.StateContentMetaInfo;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.flink.runtime.state.CheckpointedStateScope.EXCLUSIVE;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Writes channel state for a specific checkpoint-subtask-attempt triple. */
@NotThreadSafe
class ChannelStateCheckpointWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateCheckpointWriter.class);

    private final DataOutputStream dataStream;
    private final CheckpointStateOutputStream checkpointStream;
    private final ChannelStateWriteResult result;
    private final Map<InputChannelInfo, StateContentMetaInfo> inputChannelOffsets = new HashMap<>();
    private final Map<ResultSubpartitionInfo, StateContentMetaInfo> resultSubpartitionOffsets =
            new HashMap<>();
    private final ChannelStateSerializer serializer;
    private final long checkpointId;
    private boolean allInputsReceived = false;
    private boolean allOutputsReceived = false;
    private final RunnableWithException onComplete;
    private final int subtaskIndex;
    private String taskName;

    ChannelStateCheckpointWriter(
            String taskName,
            int subtaskIndex,
            CheckpointStartRequest startCheckpointItem,
            CheckpointStreamFactory streamFactory,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete)
            throws Exception {
        this(
                taskName,
                subtaskIndex,
                startCheckpointItem.getCheckpointId(),
                startCheckpointItem.getTargetResult(),
                streamFactory.createCheckpointStateOutputStream(EXCLUSIVE),
                serializer,
                onComplete);
    }

    @VisibleForTesting
    ChannelStateCheckpointWriter(
            String taskName,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriteResult result,
            CheckpointStateOutputStream stream,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete)
            throws Exception {
        this(
                taskName,
                subtaskIndex,
                checkpointId,
                result,
                serializer,
                onComplete,
                stream,
                new DataOutputStream(stream));
    }

    @VisibleForTesting
    ChannelStateCheckpointWriter(
            String taskName,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriteResult result,
            ChannelStateSerializer serializer,
            RunnableWithException onComplete,
            CheckpointStateOutputStream checkpointStateOutputStream,
            DataOutputStream dataStream)
            throws Exception {
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.checkpointId = checkpointId;
        this.result = checkNotNull(result);
        this.checkpointStream = checkNotNull(checkpointStateOutputStream);
        this.serializer = checkNotNull(serializer);
        this.dataStream = checkNotNull(dataStream);
        this.onComplete = checkNotNull(onComplete);
        runWithChecks(() -> serializer.writeHeader(dataStream));
    }

    void writeInput(InputChannelInfo info, Buffer buffer) throws Exception {
        write(
                inputChannelOffsets,
                info,
                buffer,
                !allInputsReceived,
                "ChannelStateCheckpointWriter#writeInput");
    }

    void writeOutput(ResultSubpartitionInfo info, Buffer buffer) throws Exception {
        write(
                resultSubpartitionOffsets,
                info,
                buffer,
                !allOutputsReceived,
                "ChannelStateCheckpointWriter#writeOutput");
    }

    private <K> void write(
            Map<K, StateContentMetaInfo> offsets,
            K key,
            Buffer buffer,
            boolean precondition,
            String action)
            throws Exception {
        try {
            if (result.isDone()) {
                return;
            }
            runWithChecks(
                    () -> {
                        checkState(precondition);
                        long offset = checkpointStream.getPos();
                        try (AutoCloseable ignored =
                                NetworkActionsLogger.measureIO(action, buffer)) {
                            serializer.writeData(dataStream, buffer);
                        }
                        long size = checkpointStream.getPos() - offset;
                        offsets.computeIfAbsent(key, unused -> new StateContentMetaInfo())
                                .withDataAdded(offset, size);
                        NetworkActionsLogger.tracePersist(
                                action, buffer, taskName, key, checkpointId);
                    });
        } finally {
            buffer.recycleBuffer();
        }
    }

    void completeInput() throws Exception {
        LOG.debug("complete input, output completed: {}", allOutputsReceived);
        complete(!allInputsReceived, () -> allInputsReceived = true);
    }

    void completeOutput() throws Exception {
        LOG.debug("complete output, input completed: {}", allInputsReceived);
        complete(!allOutputsReceived, () -> allOutputsReceived = true);
    }

    private void complete(boolean precondition, RunnableWithException complete) throws Exception {
        if (result.isDone()) {
            // likely after abort - only need to set the flag run onComplete callback
            doComplete(precondition, complete, onComplete);
        } else {
            runWithChecks(
                    () ->
                            doComplete(
                                    precondition,
                                    complete,
                                    onComplete,
                                    this::finishWriteAndResult));
        }
    }

    private void finishWriteAndResult() throws IOException {
        if (inputChannelOffsets.isEmpty() && resultSubpartitionOffsets.isEmpty()) {
            dataStream.close();
            result.inputChannelStateHandles.complete(emptyList());
            result.resultSubpartitionStateHandles.complete(emptyList());
            return;
        }
        dataStream.flush();
        StreamStateHandle underlying = checkpointStream.closeAndGetHandle();
        complete(
                underlying,
                result.inputChannelStateHandles,
                inputChannelOffsets,
                HandleFactory.INPUT_CHANNEL);
        complete(
                underlying,
                result.resultSubpartitionStateHandles,
                resultSubpartitionOffsets,
                HandleFactory.RESULT_SUBPARTITION);
    }

    private void doComplete(
            boolean precondition,
            RunnableWithException complete,
            RunnableWithException... callbacks)
            throws Exception {
        Preconditions.checkArgument(precondition);
        complete.run();
        if (allInputsReceived && allOutputsReceived) {
            for (RunnableWithException callback : callbacks) {
                callback.run();
            }
        }
    }

    private <I, H extends AbstractChannelStateHandle<I>> void complete(
            StreamStateHandle underlying,
            CompletableFuture<Collection<H>> future,
            Map<I, StateContentMetaInfo> offsets,
            HandleFactory<I, H> handleFactory)
            throws IOException {
        final Collection<H> handles = new ArrayList<>();
        for (Map.Entry<I, StateContentMetaInfo> e : offsets.entrySet()) {
            handles.add(createHandle(handleFactory, underlying, e.getKey(), e.getValue()));
        }
        future.complete(handles);
        LOG.debug(
                "channel state write completed, checkpointId: {}, handles: {}",
                checkpointId,
                handles);
    }

    private <I, H extends AbstractChannelStateHandle<I>> H createHandle(
            HandleFactory<I, H> handleFactory,
            StreamStateHandle underlying,
            I channelInfo,
            StateContentMetaInfo contentMetaInfo)
            throws IOException {
        Optional<byte[]> bytes =
                underlying.asBytesIfInMemory(); // todo: consider restructuring channel state and
        // removing this method:
        // https://issues.apache.org/jira/browse/FLINK-17972
        if (bytes.isPresent()) {
            StreamStateHandle extracted =
                    new ByteStreamStateHandle(
                            randomUUID().toString(),
                            serializer.extractAndMerge(bytes.get(), contentMetaInfo.getOffsets()));
            return handleFactory.create(
                    subtaskIndex,
                    channelInfo,
                    extracted,
                    singletonList(serializer.getHeaderLength()),
                    extracted.getStateSize());
        } else {
            return handleFactory.create(
                    subtaskIndex,
                    channelInfo,
                    underlying,
                    contentMetaInfo.getOffsets(),
                    contentMetaInfo.getSize());
        }
    }

    private void runWithChecks(RunnableWithException r) throws Exception {
        try {
            checkState(!result.isDone(), "result is already completed", result);
            r.run();
        } catch (Exception e) {
            fail(e);
            throw e;
        }
    }

    public void fail(Throwable e) throws Exception {
        result.fail(e);
        checkpointStream.close();
    }

    private interface HandleFactory<I, H extends AbstractChannelStateHandle<I>> {
        H create(
                int subtaskIndex,
                I info,
                StreamStateHandle underlying,
                List<Long> offsets,
                long size);

        HandleFactory<InputChannelInfo, InputChannelStateHandle> INPUT_CHANNEL =
                InputChannelStateHandle::new;

        HandleFactory<ResultSubpartitionInfo, ResultSubpartitionStateHandle> RESULT_SUBPARTITION =
                ResultSubpartitionStateHandle::new;
    }
}
