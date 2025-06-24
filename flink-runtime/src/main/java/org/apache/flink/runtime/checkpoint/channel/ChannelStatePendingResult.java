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

import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.AbstractChannelStateHandle.StateContentMetaInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The pending result of channel state for a specific checkpoint-subtask. */
public class ChannelStatePendingResult {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStatePendingResult.class);

    // Subtask information
    private final int subtaskIndex;

    private final long checkpointId;

    // Result related
    private final ChannelStateSerializer serializer;
    private final ChannelStateWriter.ChannelStateWriteResult result;
    private final Map<InputChannelInfo, AbstractChannelStateHandle.StateContentMetaInfo>
            inputChannelOffsets = new HashMap<>();
    private final Map<ResultSubpartitionInfo, AbstractChannelStateHandle.StateContentMetaInfo>
            resultSubpartitionOffsets = new HashMap<>();
    private boolean allInputsReceived = false;
    private boolean allOutputsReceived = false;

    public ChannelStatePendingResult(
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriter.ChannelStateWriteResult result,
            ChannelStateSerializer serializer) {
        this.subtaskIndex = subtaskIndex;
        this.checkpointId = checkpointId;
        this.result = result;
        this.serializer = serializer;
    }

    public boolean isAllInputsReceived() {
        return allInputsReceived;
    }

    public boolean isAllOutputsReceived() {
        return allOutputsReceived;
    }

    public Map<InputChannelInfo, StateContentMetaInfo> getInputChannelOffsets() {
        return inputChannelOffsets;
    }

    public Map<ResultSubpartitionInfo, StateContentMetaInfo> getResultSubpartitionOffsets() {
        return resultSubpartitionOffsets;
    }

    void completeInput() {
        LOG.debug("complete input, output completed: {}", allOutputsReceived);
        checkArgument(!allInputsReceived);
        allInputsReceived = true;
    }

    void completeOutput() {
        LOG.debug("complete output, input completed: {}", allInputsReceived);
        checkArgument(!allOutputsReceived);
        allOutputsReceived = true;
    }

    public void finishResult(@Nullable StreamStateHandle stateHandle) throws IOException {
        checkState(
                stateHandle != null
                        || (inputChannelOffsets.isEmpty() && resultSubpartitionOffsets.isEmpty()),
                "The stateHandle just can be null when no data is written.");
        complete(
                stateHandle,
                result.inputChannelStateHandles,
                inputChannelOffsets,
                HandleFactory.INPUT_CHANNEL);
        complete(
                stateHandle,
                result.resultSubpartitionStateHandles,
                resultSubpartitionOffsets,
                HandleFactory.RESULT_SUBPARTITION);
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

    public void fail(Throwable e) {
        result.fail(e);
    }

    public boolean isDone() {
        return this.result.isDone();
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
