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

package org.apache.flink.table.runtime.operators.rank.window;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.rank.window.combines.TopNRecordsCombiner;
import org.apache.flink.table.runtime.operators.rank.window.processors.WindowRankProcessor;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;

import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link WindowRankOperatorBuilder} is used to build a {@link SlicingWindowOperator} for window
 * rank.
 *
 * <pre>
 * SlicingWindowRankOperatorBuilder.builder()
 *   .inputSerializer(inputSerializer)
 *   .keySerializer(keySerializer)
 *   .sortKeySelector(sortKeySelector)
 *   .sortKeyComparator(genSortKeyComparator)
 *   .outputRankNumber(true)
 *   .rankStart(0)
 *   .rankEnd(100)
 *   .windowEndIndex(windowEndIndex)
 *   .build();
 * </pre>
 */
public class WindowRankOperatorBuilder {

    public static WindowRankOperatorBuilder builder() {
        return new WindowRankOperatorBuilder();
    }

    private AbstractRowDataSerializer<RowData> inputSerializer;
    private PagedTypeSerializer<RowData> keySerializer;
    private RowDataKeySelector sortKeySelector;
    private GeneratedRecordComparator generatedSortKeyComparator;
    private boolean outputRankNumber;
    private long rankStart = -1;
    private long rankEnd = -1;
    private int windowEndIndex = -1;
    private ZoneId shiftTimeZone;

    public WindowRankOperatorBuilder inputSerializer(
            AbstractRowDataSerializer<RowData> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return this;
    }

    public WindowRankOperatorBuilder shiftTimeZone(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
        return this;
    }

    public WindowRankOperatorBuilder keySerializer(PagedTypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public WindowRankOperatorBuilder sortKeySelector(RowDataKeySelector sortKeySelector) {
        this.sortKeySelector = sortKeySelector;
        return this;
    }

    public WindowRankOperatorBuilder sortKeyComparator(
            GeneratedRecordComparator genSortKeyComparator) {
        this.generatedSortKeyComparator = genSortKeyComparator;
        return this;
    }

    public WindowRankOperatorBuilder outputRankNumber(boolean outputRankNumber) {
        this.outputRankNumber = outputRankNumber;
        return this;
    }

    public WindowRankOperatorBuilder rankStart(long rankStart) {
        this.rankStart = rankStart;
        return this;
    }

    public WindowRankOperatorBuilder rankEnd(long rankEnd) {
        this.rankEnd = rankEnd;
        return this;
    }

    public WindowRankOperatorBuilder windowEndIndex(int windowEndIndex) {
        this.windowEndIndex = windowEndIndex;
        return this;
    }

    public SlicingWindowOperator<RowData, ?> build() {
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkNotNull(sortKeySelector);
        checkNotNull(generatedSortKeyComparator);
        checkArgument(
                rankStart > 0,
                String.format("Illegal rank start %s, it should be positive!", rankStart));
        checkArgument(
                rankEnd >= 1,
                String.format("Illegal rank end %s, it should be at least 1!", rankEnd));
        checkArgument(
                rankEnd >= rankStart,
                String.format(
                        "Illegal rank start %s and rank end %s, rank start should not be bigger than or equal to rank end!",
                        rankStart, rankEnd));
        checkArgument(
                windowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!", windowEndIndex));
        final RecordsCombiner.Factory combinerFactory =
                new TopNRecordsCombiner.Factory(
                        generatedSortKeyComparator, sortKeySelector, inputSerializer, rankEnd);
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keySerializer, inputSerializer, combinerFactory);
        final SlicingWindowProcessor<Long> windowProcessor =
                new WindowRankProcessor(
                        inputSerializer,
                        generatedSortKeyComparator,
                        sortKeySelector.getProducedType().toSerializer(),
                        bufferFactory,
                        rankStart,
                        rankEnd,
                        outputRankNumber,
                        windowEndIndex,
                        shiftTimeZone);
        return new SlicingWindowOperator<>(windowProcessor);
    }
}
