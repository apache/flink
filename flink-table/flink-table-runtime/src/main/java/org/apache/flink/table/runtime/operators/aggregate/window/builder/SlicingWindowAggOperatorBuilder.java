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

package org.apache.flink.table.runtime.operators.aggregate.window.builder;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.AggCombiner;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.GlobalAggCombiner;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceSharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceUnsharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.windowtvf.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.windowtvf.common.AbstractWindowOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceAssigners.HoppingSliceAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceSharedAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SliceUnsharedAssigner;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.operators.window.windowtvf.slicing.SlicingWindowProcessor;

import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link SlicingWindowAggOperatorBuilder} is used to build a {@link SlicingWindowOperator} for
 * window aggregate.
 *
 * <pre>
 * SlicingWindowAggOperatorBuilder.builder()
 *   .inputType(inputType)
 *   .keyTypes(keyFieldTypes)
 *   .sliceAssigner(SliceAssigners.tumbling(rowtimeIndex, Duration.ofSeconds(5)))
 *   .aggregate(genAggsFunction), accTypes)
 *   .build();
 * </pre>
 */
public class SlicingWindowAggOperatorBuilder
        extends AbstractWindowAggOperatorBuilder<Long, SlicingWindowAggOperatorBuilder> {

    public static SlicingWindowAggOperatorBuilder builder() {
        return new SlicingWindowAggOperatorBuilder();
    }

    private SliceAssigner sliceAssigner;

    private int indexOfCountStart = -1;

    public SlicingWindowAggOperatorBuilder sliceAssigner(SliceAssigner sliceAssigner) {
        this.sliceAssigner = sliceAssigner;
        return this;
    }

    /**
     * Specify the index position of the COUNT(*) value in the accumulator buffer. This is only
     * required for Hopping windows which uses this to determine whether the window is empty and
     * then decide whether to register timer for the next window.
     *
     * @see HoppingSliceAssigner#nextTriggerWindow(long, Supplier)
     */
    public SlicingWindowAggOperatorBuilder countStarIndex(int indexOfCountStart) {
        this.indexOfCountStart = indexOfCountStart;
        return self();
    }

    @Override
    protected AbstractWindowOperator<RowData, ?> buildInner() {
        checkNotNull(sliceAssigner);
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkNotNull(accSerializer);
        checkNotNull(generatedAggregateFunction);

        boolean isGlobalAgg =
                localGeneratedAggregateFunction != null && globalGeneratedAggregateFunction != null;

        RecordsCombiner.Factory combinerFactory;
        if (isGlobalAgg) {
            combinerFactory =
                    new GlobalAggCombiner.Factory(
                            localGeneratedAggregateFunction, globalGeneratedAggregateFunction);
        } else {
            combinerFactory = new AggCombiner.Factory(generatedAggregateFunction);
        }
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keySerializer, inputSerializer, combinerFactory);

        final SlicingWindowProcessor<Long> windowProcessor;
        if (sliceAssigner instanceof SliceSharedAssigner) {
            windowProcessor =
                    new SliceSharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            (SliceSharedAssigner) sliceAssigner,
                            accSerializer,
                            indexOfCountStart,
                            shiftTimeZone);
        } else if (sliceAssigner instanceof SliceUnsharedAssigner) {
            windowProcessor =
                    new SliceUnsharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            (SliceUnsharedAssigner) sliceAssigner,
                            accSerializer,
                            indexOfCountStart,
                            shiftTimeZone);
        } else {
            throw new IllegalArgumentException(
                    "assigner must be instance of SliceUnsharedAssigner or SliceSharedAssigner.");
        }
        return new SlicingWindowOperator<>(windowProcessor);
    }

    @Override
    protected SlicingWindowAggOperatorBuilder self() {
        return this;
    }
}
