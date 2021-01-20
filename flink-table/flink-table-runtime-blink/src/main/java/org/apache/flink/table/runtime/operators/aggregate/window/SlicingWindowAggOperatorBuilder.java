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

package org.apache.flink.table.runtime.operators.aggregate.window;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.CombineRecordsFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceSharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceUnsharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners.HoppingSliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceSharedAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceUnsharedAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowProcessor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

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
 *   .assigner(SliceAssigners.tumbling(rowtimeIndex, Duration.ofSeconds(5)))
 *   .aggregate(genAggsFunction), accTypes)
 *   .build();
 * </pre>
 */
public class SlicingWindowAggOperatorBuilder {

    public static SlicingWindowAggOperatorBuilder builder() {
        return new SlicingWindowAggOperatorBuilder();
    }

    private SliceAssigner assigner;
    private RowType inputType;
    private LogicalType[] keyTypes;
    private LogicalType[] accumulatorTypes;
    private GeneratedNamespaceAggsHandleFunction<Long> generatedAggregateFunction;
    private int indexOfCountStart = -1;

    public SlicingWindowAggOperatorBuilder inputType(RowType rowType) {
        this.inputType = rowType;
        return this;
    }

    public SlicingWindowAggOperatorBuilder keyTypes(LogicalType[] keyTypes) {
        this.keyTypes = keyTypes;
        return this;
    }

    public SlicingWindowAggOperatorBuilder assigner(SliceAssigner assigner) {
        this.assigner = assigner;
        return this;
    }

    public SlicingWindowAggOperatorBuilder aggregate(
            GeneratedNamespaceAggsHandleFunction<Long> generatedAggregateFunction,
            LogicalType[] accumulatorTypes) {
        this.generatedAggregateFunction = generatedAggregateFunction;
        this.accumulatorTypes = accumulatorTypes;
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
        return this;
    }

    public SlicingWindowOperator<RowData, ?> build() {
        checkNotNull(assigner);
        checkNotNull(inputType);
        checkNotNull(keyTypes);
        checkNotNull(accumulatorTypes);
        checkNotNull(generatedAggregateFunction);
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keyTypes, inputType);
        final WindowCombineFunction.Factory combinerFactory =
                new CombineRecordsFunction.Factory(generatedAggregateFunction);
        final SlicingWindowProcessor<Long> windowProcessor;
        if (assigner instanceof SliceSharedAssigner) {
            windowProcessor =
                    new SliceSharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            combinerFactory,
                            (SliceSharedAssigner) assigner,
                            accumulatorTypes,
                            indexOfCountStart);
        } else if (assigner instanceof SliceUnsharedAssigner) {
            windowProcessor =
                    new SliceUnsharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            combinerFactory,
                            (SliceUnsharedAssigner) assigner,
                            accumulatorTypes);
        } else {
            throw new IllegalArgumentException(
                    "assigner must be instance of SliceUnsharedAssigner or SliceSharedAssigner.");
        }
        return new SlicingWindowOperator<>(windowProcessor);
    }
}
