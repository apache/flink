/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.rank.window.processors;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.slicing.WindowTimerServiceImpl;
import org.apache.flink.table.runtime.operators.window.state.WindowMapState;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.isWindowFired;

/** An window rank processor. */
public final class WindowRankProcessor implements SlicingWindowProcessor<Long> {
    private static final long serialVersionUID = 1L;

    private final GeneratedRecordComparator generatedSortKeyComparator;

    /** The util to compare two sortKey equals to each other. */
    private Comparator<RowData> sortKeyComparator;

    private final TypeSerializer<RowData> sortKeySerializer;

    private final WindowBuffer.Factory bufferFactory;
    private final TypeSerializer<RowData> inputSerializer;
    private final long rankStart;
    private final long rankEnd;
    private final boolean outputRankNumber;
    private final int windowEndIndex;
    private final ZoneId shiftTimeZone;

    // ----------------------------------------------------------------------------------------

    private transient long currentProgress;

    private transient Context<Long> ctx;

    private transient WindowTimerService<Long> windowTimerService;

    private transient WindowBuffer windowBuffer;

    /** state schema: [key, window_end, sort key, records]. */
    private transient WindowMapState<Long, List<RowData>> windowState;

    private transient JoinedRowData reuseOutput;
    private transient GenericRowData reuseRankRow;

    public WindowRankProcessor(
            TypeSerializer<RowData> inputSerializer,
            GeneratedRecordComparator genSortKeyComparator,
            TypeSerializer<RowData> sortKeySerializer,
            WindowBuffer.Factory bufferFactory,
            long rankStart,
            long rankEnd,
            boolean outputRankNumber,
            int windowEndIndex,
            ZoneId shiftTimeZone) {
        this.inputSerializer = inputSerializer;
        this.generatedSortKeyComparator = genSortKeyComparator;
        this.sortKeySerializer = sortKeySerializer;
        this.bufferFactory = bufferFactory;
        this.rankStart = rankStart;
        this.rankEnd = rankEnd;
        this.outputRankNumber = outputRankNumber;
        this.windowEndIndex = windowEndIndex;
        this.shiftTimeZone = shiftTimeZone;
    }

    @Override
    public void open(Context<Long> context) throws Exception {
        this.ctx = context;

        // compile comparator
        sortKeyComparator =
                generatedSortKeyComparator.newInstance(
                        ctx.getRuntimeContext().getUserCodeClassLoader());

        final LongSerializer namespaceSerializer = LongSerializer.INSTANCE;
        ListSerializer<RowData> listSerializer = new ListSerializer<>(inputSerializer);
        MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor =
                new MapStateDescriptor<>("window_rank", sortKeySerializer, listSerializer);
        MapState<RowData, List<RowData>> state =
                ctx.getKeyedStateBackend()
                        .getOrCreateKeyedState(namespaceSerializer, mapStateDescriptor);

        this.windowTimerService = new WindowTimerServiceImpl(ctx.getTimerService(), shiftTimeZone);
        this.windowState =
                new WindowMapState<>(
                        (InternalMapState<RowData, Long, RowData, List<RowData>>) state);
        this.windowBuffer =
                bufferFactory.create(
                        ctx.getOperatorOwner(),
                        ctx.getMemoryManager(),
                        ctx.getMemorySize(),
                        ctx.getRuntimeContext(),
                        windowTimerService,
                        ctx.getKeyedStateBackend(),
                        windowState,
                        true,
                        shiftTimeZone);

        this.reuseOutput = new JoinedRowData();
        this.reuseRankRow = new GenericRowData(1);
        this.currentProgress = Long.MIN_VALUE;
    }

    @Override
    public boolean processElement(RowData key, RowData element) throws Exception {
        long sliceEnd = element.getLong(windowEndIndex);
        if (isWindowFired(sliceEnd, currentProgress, shiftTimeZone)) {
            // element is late and should be dropped
            return true;
        }
        windowBuffer.addElement(key, sliceEnd, element);
        return false;
    }

    @Override
    public void advanceProgress(long progress) throws Exception {
        if (progress > currentProgress) {
            currentProgress = progress;
            windowBuffer.advanceProgress(currentProgress);
        }
    }

    @Override
    public void prepareCheckpoint() throws Exception {
        windowBuffer.flush();
    }

    @Override
    public void clearWindow(Long windowEnd) throws Exception {
        windowState.clear(windowEnd);
    }

    @Override
    public void close() throws Exception {
        if (windowBuffer != null) {
            windowBuffer.close();
        }
    }

    @Override
    public TypeSerializer<Long> createWindowSerializer() {
        return LongSerializer.INSTANCE;
    }

    @Override
    public void fireWindow(Long windowEnd) throws Exception {
        TopNBuffer buffer = new TopNBuffer(sortKeyComparator, ArrayList::new);
        // step 1: load state data into TopNBuffer
        Iterator<Map.Entry<RowData, List<RowData>>> stateIterator = windowState.iterator(windowEnd);
        while (stateIterator.hasNext()) {
            Map.Entry<RowData, List<RowData>> entry = stateIterator.next();
            RowData sortKey = entry.getKey();
            if (buffer.checkSortKeyInBufferRange(sortKey, rankEnd)) {
                buffer.putAll(sortKey, entry.getValue());
            }
        }

        // step 2: send [rankStart, rankEnd] result
        Iterator<Map.Entry<RowData, Collection<RowData>>> bufferItr = buffer.entrySet().iterator();
        long currentRank = 1L;
        while (bufferItr.hasNext() && currentRank <= rankEnd) {
            Map.Entry<RowData, Collection<RowData>> entry = bufferItr.next();
            Collection<RowData> records = entry.getValue();
            Iterator<RowData> recordsIter = records.iterator();
            while (recordsIter.hasNext() && currentRank <= rankEnd) {
                RowData rowData = recordsIter.next();
                if (currentRank >= rankStart && currentRank <= rankEnd) {
                    ctx.output(createOutputRow(rowData, currentRank));
                }
                currentRank += 1;
            }
        }
    }

    private RowData createOutputRow(RowData inputRow, long rank) {
        if (outputRankNumber) {
            reuseRankRow.setField(0, rank);
            reuseOutput.replace(inputRow, reuseRankRow);
            reuseOutput.setRowKind(RowKind.INSERT);
            return reuseOutput;
        } else {
            return inputRow;
        }
    }
}
