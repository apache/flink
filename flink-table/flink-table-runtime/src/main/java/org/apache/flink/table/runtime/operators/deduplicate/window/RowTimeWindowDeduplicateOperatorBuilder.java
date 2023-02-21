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

package org.apache.flink.table.runtime.operators.deduplicate.window;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.deduplicate.window.combines.RowTimeDeduplicateRecordsCombiner;
import org.apache.flink.table.runtime.operators.deduplicate.window.processors.RowTimeWindowDeduplicateProcessor;
import org.apache.flink.table.runtime.operators.window.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;

import java.time.ZoneId;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link RowTimeWindowDeduplicateOperatorBuilder} is used to build a {@link
 * SlicingWindowOperator} for rowtime window deduplicate.
 *
 * <pre>
 * RowTimeWindowDeduplicateOperatorBuilder.builder()
 *   .inputSerializer(inputSerializer)
 *   .keySerializer(keySerializer)
 *   .keepLastRow(true)
 *   .rowtimeIndex(0)
 *   .windowEndIndex(windowEndIndex)
 *   .build();
 * </pre>
 */
public class RowTimeWindowDeduplicateOperatorBuilder {

    public static RowTimeWindowDeduplicateOperatorBuilder builder() {
        return new RowTimeWindowDeduplicateOperatorBuilder();
    }

    private AbstractRowDataSerializer<RowData> inputSerializer;
    private PagedTypeSerializer<RowData> keySerializer;
    private int rowtimeIndex;
    private int windowEndIndex = -1;
    private ZoneId shiftTimeZone;
    private boolean keepLastRow;

    public RowTimeWindowDeduplicateOperatorBuilder inputSerializer(
            AbstractRowDataSerializer<RowData> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return this;
    }

    public RowTimeWindowDeduplicateOperatorBuilder shiftTimeZone(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
        return this;
    }

    public RowTimeWindowDeduplicateOperatorBuilder keySerializer(
            PagedTypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public RowTimeWindowDeduplicateOperatorBuilder keepLastRow(boolean keepLastRow) {
        this.keepLastRow = keepLastRow;
        return this;
    }

    public RowTimeWindowDeduplicateOperatorBuilder rowtimeIndex(int rowtimeIndex) {
        this.rowtimeIndex = rowtimeIndex;
        return this;
    }

    public RowTimeWindowDeduplicateOperatorBuilder windowEndIndex(int windowEndIndex) {
        this.windowEndIndex = windowEndIndex;
        return this;
    }

    public SlicingWindowOperator<RowData, ?> build() {
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkArgument(
                windowEndIndex >= 0,
                String.format(
                        "Illegal window end index %s, it should not be negative!", windowEndIndex));
        final RecordsCombiner.Factory combinerFactory =
                new RowTimeDeduplicateRecordsCombiner.Factory(
                        inputSerializer, rowtimeIndex, keepLastRow);
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keySerializer, inputSerializer, combinerFactory);
        final SlicingWindowProcessor<Long> windowProcessor =
                new RowTimeWindowDeduplicateProcessor(
                        inputSerializer, bufferFactory, windowEndIndex, shiftTimeZone);
        return new SlicingWindowOperator<>(windowProcessor);
    }
}
