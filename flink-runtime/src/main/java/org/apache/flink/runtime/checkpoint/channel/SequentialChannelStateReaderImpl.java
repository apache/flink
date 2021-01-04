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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link SequentialChannelStateReader} implementation. */
public class SequentialChannelStateReaderImpl implements SequentialChannelStateReader {

    private final TaskStateSnapshot taskStateSnapshot;
    private final ChannelStateSerializer serializer;
    private final ChannelStateChunkReader chunkReader;

    public SequentialChannelStateReaderImpl(TaskStateSnapshot taskStateSnapshot) {
        this.taskStateSnapshot = taskStateSnapshot;
        this.serializer = new ChannelStateSerializerImpl();
        this.chunkReader = new ChannelStateChunkReader(serializer);
    }

    @Override
    public void readInputData(InputGate[] inputGates) throws IOException, InterruptedException {
        try (InputChannelRecoveredStateHandler stateHandler =
                new InputChannelRecoveredStateHandler(inputGates)) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(), OperatorSubtaskState::getInputChannelState));
        }
    }

    @Override
    public void readOutputData(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion)
            throws IOException, InterruptedException {
        try (ResultSubpartitionRecoveredStateHandler stateHandler =
                new ResultSubpartitionRecoveredStateHandler(writers, notifyAndBlockOnCompletion)) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            OperatorSubtaskState::getResultSubpartitionState));
        }
    }

    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void read(
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Map<StreamStateHandle, List<Handle>> streamStateHandleListMap)
            throws IOException, InterruptedException {
        for (Map.Entry<StreamStateHandle, List<Handle>> delegateAndHandles :
                streamStateHandleListMap.entrySet()) {
            readSequentially(
                    delegateAndHandles.getKey(), delegateAndHandles.getValue(), stateHandler);
        }
    }

    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void readSequentially(
            StreamStateHandle streamStateHandle,
            List<Handle> channelStateHandles,
            RecoveredChannelStateHandler<Info, Context> stateHandler)
            throws IOException, InterruptedException {
        try (FSDataInputStream is = streamStateHandle.openInputStream()) {
            serializer.readHeader(is);
            for (Tuple2<Long, Info> offsetAndChannelInfo :
                    extractOffsetsSorted(channelStateHandles)) {
                chunkReader.readChunk(
                        is, offsetAndChannelInfo.f0, stateHandler, offsetAndChannelInfo.f1);
            }
        }
    }

    private Stream<OperatorSubtaskState> streamSubtaskStates() {
        return taskStateSnapshot.getSubtaskStateMappings().stream().map(Map.Entry::getValue);
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Map<StreamStateHandle, List<Handle>> groupByDelegate(
                    Stream<OperatorSubtaskState> states,
                    Function<OperatorSubtaskState, StateObjectCollection<Handle>>
                            stateHandleExtractor) {
        return states.map(stateHandleExtractor)
                .flatMap(Collection::stream)
                .peek(validate())
                .collect(groupingBy(AbstractChannelStateHandle::getDelegate));
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Consumer<Handle> validate() {
        Set<Info> seen = new HashSet<>();
        // expect each channel to be described only once; otherwise, buffers in channel could be
        // re-ordered
        return handle -> checkState(seen.add(handle.getInfo()), "duplicate channel info: %s");
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            List<Tuple2<Long, Info>> extractOffsetsSorted(List<Handle> channelStateHandles) {
        return channelStateHandles.stream()
                .flatMap(SequentialChannelStateReaderImpl::extractOffsets)
                .sorted(comparingLong(offsetAndInfo -> offsetAndInfo.f0))
                .collect(toList());
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Stream<Tuple2<Long, Info>> extractOffsets(Handle handle) {
        return handle.getOffsets().stream().map(offset -> Tuple2.of(offset, handle.getInfo()));
    }

    @Override
    public void close() throws Exception {}
}

class ChannelStateChunkReader {
    private final ChannelStateSerializer serializer;

    ChannelStateChunkReader(ChannelStateSerializer serializer) {
        this.serializer = serializer;
    }

    <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void readChunk(
            FSDataInputStream source,
            long sourceOffset,
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Info channelInfo)
            throws IOException, InterruptedException {
        if (source.getPos() != sourceOffset) {
            source.seek(sourceOffset);
        }
        int length = serializer.readLength(source);
        while (length > 0) {
            RecoveredChannelStateHandler.BufferWithContext<Context> bufferWithContext =
                    stateHandler.getBuffer(channelInfo);
            try {
                while (length > 0 && bufferWithContext.buffer.isWritable()) {
                    length -= serializer.readData(source, bufferWithContext.buffer, length);
                }
            } catch (Exception e) {
                bufferWithContext.buffer.recycle();
                throw e;
            }
            stateHandler.recover(channelInfo, bufferWithContext.context);
        }
    }
}
