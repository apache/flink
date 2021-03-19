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

import org.apache.flink.api.common.typeutils.TypeSerializer;
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
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;

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
    private AbstractRowDataSerializer<RowData> inputSerializer;
    private PagedTypeSerializer<RowData> keySerializer;
    private TypeSerializer<RowData> accSerializer;
    private GeneratedNamespaceAggsHandleFunction<Long> generatedAggregateFunction;
    private int indexOfCountStart = -1;

    public SlicingWindowAggOperatorBuilder inputSerializer(
            AbstractRowDataSerializer<RowData> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return this;
    }

    public SlicingWindowAggOperatorBuilder keySerializer(
            PagedTypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public SlicingWindowAggOperatorBuilder assigner(SliceAssigner assigner) {
        this.assigner = assigner;
        return this;
    }

    public SlicingWindowAggOperatorBuilder aggregate(
            GeneratedNamespaceAggsHandleFunction<Long> generatedAggregateFunction,
            TypeSerializer<RowData> accSerializer) {
        this.generatedAggregateFunction = generatedAggregateFunction;
        this.accSerializer = accSerializer;
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
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkNotNull(accSerializer);
        checkNotNull(generatedAggregateFunction);
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keySerializer, inputSerializer);
        final WindowCombineFunction.Factory combinerFactory =
                new CombineRecordsFunction.Factory(
                        generatedAggregateFunction, keySerializer, inputSerializer);
        final SlicingWindowProcessor<Long> windowProcessor;
        if (assigner instanceof SliceSharedAssigner) {
            windowProcessor =
                    new SliceSharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            combinerFactory,
                            (SliceSharedAssigner) assigner,
                            accSerializer,
                            indexOfCountStart);
        } else if (assigner instanceof SliceUnsharedAssigner) {
            windowProcessor =
                    new SliceUnsharedWindowAggProcessor(
                            generatedAggregateFunction,
                            bufferFactory,
                            combinerFactory,
                            (SliceUnsharedAssigner) assigner,
                            accSerializer);
        } else {
            throw new IllegalArgumentException(
                    "assigner must be instance of SliceUnsharedAssigner or SliceSharedAssigner.");
        }
        return new SlicingWindowOperator<>(windowProcessor);
    }
}
