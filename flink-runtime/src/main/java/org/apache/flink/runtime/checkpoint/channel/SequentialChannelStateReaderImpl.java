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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.ChannelStateHelper;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
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
        serializer = new ChannelStateSerializerImpl();
        chunkReader = new ChannelStateChunkReader(serializer);
    }

    @Override
    public void readInputData(InputGate[] inputGates, RecordFilterContext filterContext)
            throws IOException, InterruptedException {

        // Create filtering handler if filtering is needed
        ChannelStateFilteringHandler filteringHandler =
                filterContext.isCheckpointingDuringRecoveryEnabled()
                        ? ChannelStateFilteringHandler.createFromContext(filterContext, inputGates)
                        : null;

        // Create per-channel stores and FilteredBufferDispatcher when filtering is enabled
        FilteredBufferDispatcher dispatcher = null;
        if (filteringHandler != null) {
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel =
                    createPerChannelStores(inputGates);
            if (!storesByChannel.isEmpty()) {
                dispatcher =
                        createFilteredBufferDispatcher(inputGates, storesByChannel, filterContext);
            }
        }

        try (ChannelStateFilteringHandler ignored = filteringHandler;
                FilteredBufferDispatcher d = dispatcher;
                InputChannelRecoveredStateHandler stateHandler =
                        new InputChannelRecoveredStateHandler(
                                inputGates,
                                taskStateSnapshot.getInputRescalingDescriptor(),
                                filteringHandler,
                                filterContext.getMemorySegmentSize(),
                                dispatcher)) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            ChannelStateHelper::extractUnmergedInputHandles));
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            OperatorSubtaskState::getUpstreamOutputBufferState));

            if (filteringHandler != null) {
                checkState(
                        !filteringHandler.hasPartialData(),
                        "Not all data has been fully consumed during filtering");
            }

            if (d != null) {
                d.flush();
                // Explicit call sequence (see requirements/38544/close_drain_separation.md):
                //   flush()              -> seal Readers
                //   finishRecovery()     -> trigger channel conversion
                //   drainPendingSpill()  -> blocking drain (no monitor held)
                // try-with-resources still owns the final close() pass for resource release.
                stateHandler.finishRecovery();
                d.drainPendingSpill();
            } else {
                stateHandler.finishRecovery();
            }
            // try-with-resources auto-close is a pure resource release pass:
            //   stateHandler.close() -> releases preFilterSegment if allocated
            //   d.close()            -> deletes spill files / closes file channel
            //   filteringHandler.close()
        }
    }

    @Override
    public void readOutputData(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion)
            throws IOException, InterruptedException {
        try (ResultSubpartitionRecoveredStateHandler stateHandler =
                new ResultSubpartitionRecoveredStateHandler(
                        writers,
                        notifyAndBlockOnCompletion,
                        taskStateSnapshot.getOutputRescalingDescriptor())) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            ChannelStateHelper::extractUnmergedOutputHandles));
            stateHandler.finishRecovery();
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
            for (RescaledOffset<Info> offsetAndChannelInfo :
                    extractOffsetsSorted(channelStateHandles)) {
                chunkReader.readChunk(
                        is,
                        offsetAndChannelInfo.offset,
                        stateHandler,
                        offsetAndChannelInfo.channelInfo,
                        offsetAndChannelInfo.oldSubtaskIndex);
            }
        }
    }

    /**
     * Creates a RecoveredBufferStoreImpl for each RecoveredInputChannel and wires the notification
     * callback. Only called when filtering is enabled.
     */
    private Map<InputChannelInfo, RecoveredBufferStoreImpl> createPerChannelStores(
            InputGate[] inputGates) {
        Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel = new HashMap<>();
        for (InputGate gate : inputGates) {
            for (int i = 0; i < gate.getNumberOfInputChannels(); i++) {
                InputChannel ch = gate.getChannel(i);
                if (ch instanceof RecoveredInputChannel) {
                    RecoveredInputChannel recoveredCh = (RecoveredInputChannel) ch;
                    InputChannelInfo info = recoveredCh.getChannelInfo();
                    // RecoveredInputChannel already creates its own store internally and wires the
                    // notification callback. Reuse the existing store so that all paths (non-
                    // filtering and filtering) deliver to the same store instance.
                    RecoveredBufferStoreImpl store = recoveredCh.getStore();
                    storesByChannel.put(info, store);
                }
            }
        }
        return storesByChannel;
    }

    /**
     * Creates a {@link FilteredBufferDispatcher} from the per-channel stores. The requester is
     * keyed by {@link InputChannelInfo} so each channel draws from its own pool.
     */
    private FilteredBufferDispatcher createFilteredBufferDispatcher(
            InputGate[] inputGates,
            Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel,
            RecordFilterContext filterContext)
            throws IOException {
        Map<InputChannelInfo, RecoveredInputChannel> channelMap = buildChannelMap(inputGates);
        String[] spillDirs = filterContext.getTmpDirectories();
        int memorySegmentSize = filterContext.getMemorySegmentSize();

        // Use the channelStateWriter from any channel (all channels share the same writer).
        RecoveredInputChannel anyChannel = channelMap.values().iterator().next();

        return new FilteredBufferDispatcherImpl(
                storesByChannel,
                anyChannel.getChannelStateWriter(),
                spillDirs,
                memorySegmentSize,
                new RecoveredChannelBufferRequester(channelMap));
    }

    /**
     * {@link BufferRequester} backed by a {@link RecoveredInputChannel} map, routing each channel's
     * request to its own exclusive buffer pool.
     */
    private static final class RecoveredChannelBufferRequester implements BufferRequester {

        private final Map<InputChannelInfo, RecoveredInputChannel> channelMap;

        RecoveredChannelBufferRequester(Map<InputChannelInfo, RecoveredInputChannel> channelMap) {
            this.channelMap = channelMap;
        }

        @Override
        public Buffer requestBuffer(InputChannelInfo channelInfo) throws IOException {
            return lookup(channelInfo).requestBuffer();
        }

        @Override
        public Buffer requestBufferBlocking(InputChannelInfo channelInfo)
                throws InterruptedException, IOException {
            return lookup(channelInfo).requestBufferBlocking();
        }

        @Override
        public void releaseExclusiveBuffers() throws IOException {
            // Drain is finished. Mark each channel's drain-done flag; the actual release fires
            // in a two-flag rendezvous (drainDone + converted) inside RecoveredInputChannel —
            // releasing here directly would race with mailbox-driven convertRecoveredInputChannels
            // and cause task pollNext on the not-yet-replaced gate slot to hit the released-channel
            // checkState in RecoveredInputChannel#getNextRecoveredStateBuffer.
            for (RecoveredInputChannel ch : channelMap.values()) {
                ch.markDrainDone();
            }
        }

        private RecoveredInputChannel lookup(InputChannelInfo channelInfo) {
            RecoveredInputChannel ch = channelMap.get(channelInfo);
            if (ch == null) {
                throw new IllegalArgumentException(
                        "No RecoveredInputChannel for channelInfo: " + channelInfo);
            }
            return ch;
        }
    }

    private Map<InputChannelInfo, RecoveredInputChannel> buildChannelMap(InputGate[] inputGates) {
        Map<InputChannelInfo, RecoveredInputChannel> channelMap = new HashMap<>();
        for (InputGate gate : inputGates) {
            for (int i = 0; i < gate.getNumberOfInputChannels(); i++) {
                InputChannel ch = gate.getChannel(i);
                if (ch instanceof RecoveredInputChannel) {
                    RecoveredInputChannel recoveredCh = (RecoveredInputChannel) ch;
                    channelMap.put(recoveredCh.getChannelInfo(), recoveredCh);
                }
            }
        }
        return channelMap;
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
        Set<Tuple2<Info, Integer>> seen = new HashSet<>();
        // expect each channel/subtask to be described only once; otherwise, buffers in channel
        // could be
        // re-ordered
        return handle -> {
            if (!seen.add(new Tuple2<>(handle.getInfo(), handle.getSubtaskIndex()))) {
                throw new IllegalStateException("Duplicate channel info: " + handle);
            }
        };
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            List<RescaledOffset<Info>> extractOffsetsSorted(List<Handle> channelStateHandles) {
        return channelStateHandles.stream()
                .flatMap(SequentialChannelStateReaderImpl::extractOffsets)
                .sorted(comparingLong(offsetAndInfo -> offsetAndInfo.offset))
                .collect(toList());
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Stream<RescaledOffset<Info>> extractOffsets(Handle handle) {
        return handle.getOffsets().stream()
                .map(
                        offset ->
                                new RescaledOffset<>(
                                        offset, handle.getInfo(), handle.getSubtaskIndex()));
    }

    @Override
    public void close() throws Exception {}

    static class RescaledOffset<Info> {
        final Long offset;
        final Info channelInfo;
        final int oldSubtaskIndex;

        RescaledOffset(Long offset, Info channelInfo, int oldSubtaskIndex) {
            this.offset = offset;
            this.channelInfo = channelInfo;
            this.oldSubtaskIndex = oldSubtaskIndex;
        }
    }
}

class ChannelStateChunkReader {
    private final ChannelStateSerializer serializer;

    ChannelStateChunkReader(ChannelStateSerializer serializer) {
        this.serializer = serializer;
    }

    <Info, Context> void readChunk(
            FSDataInputStream source,
            long sourceOffset,
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Info channelInfo,
            int oldSubtaskIndex)
            throws IOException, InterruptedException {
        if (source.getPos() != sourceOffset) {
            source.seek(sourceOffset);
        }
        int length = serializer.readLength(source);
        while (length > 0) {
            RecoveredChannelStateHandler.BufferWithContext<Context> bufferWithContext =
                    stateHandler.getBuffer(channelInfo);
            try (Closeable ignored =
                    NetworkActionsLogger.measureIO(
                            "ChannelStateChunkReader#readChunk", bufferWithContext.buffer)) {
                while (length > 0 && bufferWithContext.buffer.isWritable()) {
                    length -= serializer.readData(source, bufferWithContext.buffer, length);
                }
            } catch (Exception e) {
                bufferWithContext.close();
                throw e;
            }

            // Passing the ownership of buffer to inside.
            stateHandler.recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }
}
