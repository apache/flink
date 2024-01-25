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
import org.apache.flink.table.runtime.operators.aggregate.window.combines.AggCombiner;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.GlobalAggCombiner;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceSharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceUnsharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.UnsliceWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.tvf.combines.RecordsCombiner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners.HoppingSliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceSharedAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceUnsharedAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SlicingWindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnsliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.unslicing.UnslicingWindowProcessor;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;

import java.time.ZoneId;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link WindowAggOperatorBuilder} is used to build a {@link WindowAggOperator} with {@link
 * SlicingWindowProcessor} or a {@link UnslicingWindowProcessor}.
 *
 * <pre>
 * WindowAggOperatorBuilder.builder()
 *   .inputType(inputType)
 *   .keyTypes(keyFieldTypes)
 *   .assigner(SliceAssigners.tumbling(rowtimeIndex, Duration.ofSeconds(5)))
 *   .aggregate(genAggsFunction), accTypes)
 *   .build();
 * </pre>
 *
 * <p>or
 *
 * <pre>
 * WindowAggOperatorBuilder.builder()
 *   .inputType(inputType)
 *   .keyTypes(keyFieldTypes)
 *   .assigner(UnsliceAssigners.session(rowtimeIndex, Duration.ofSeconds(5)))
 *   .aggregate(genAggsFunction), accTypes)
 *   .build();
 * </pre>
 */
public class WindowAggOperatorBuilder {

    public static WindowAggOperatorBuilder builder() {
        return new WindowAggOperatorBuilder();
    }

    private WindowAssigner assigner;
    private AbstractRowDataSerializer<RowData> inputSerializer;
    private PagedTypeSerializer<RowData> keySerializer;
    private AbstractRowDataSerializer<RowData> accSerializer;
    private GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction;
    private GeneratedNamespaceAggsHandleFunction<?> localGeneratedAggregateFunction;
    private GeneratedNamespaceAggsHandleFunction<?> globalGeneratedAggregateFunction;
    private int indexOfCountStart = -1;
    private ZoneId shiftTimeZone;

    public WindowAggOperatorBuilder inputSerializer(
            AbstractRowDataSerializer<RowData> inputSerializer) {
        this.inputSerializer = inputSerializer;
        return this;
    }

    public WindowAggOperatorBuilder shiftTimeZone(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
        return this;
    }

    public WindowAggOperatorBuilder keySerializer(PagedTypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    public WindowAggOperatorBuilder assigner(WindowAssigner assigner) {
        this.assigner = assigner;
        return this;
    }

    public WindowAggOperatorBuilder aggregate(
            GeneratedNamespaceAggsHandleFunction<?> generatedAggregateFunction,
            AbstractRowDataSerializer<RowData> accSerializer) {
        this.generatedAggregateFunction = generatedAggregateFunction;
        this.accSerializer = accSerializer;
        return this;
    }

    public WindowAggOperatorBuilder globalAggregate(
            GeneratedNamespaceAggsHandleFunction<?> localGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<?> globalGeneratedAggregateFunction,
            GeneratedNamespaceAggsHandleFunction<?> stateGeneratedAggregateFunction,
            AbstractRowDataSerializer<RowData> accSerializer) {
        this.localGeneratedAggregateFunction = localGeneratedAggregateFunction;
        this.globalGeneratedAggregateFunction = globalGeneratedAggregateFunction;
        this.generatedAggregateFunction = stateGeneratedAggregateFunction;
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
    public WindowAggOperatorBuilder countStarIndex(int indexOfCountStart) {
        this.indexOfCountStart = indexOfCountStart;
        return this;
    }

    public WindowAggOperator<RowData, ?> build() {
        checkNotNull(assigner);
        checkNotNull(inputSerializer);
        checkNotNull(keySerializer);
        checkNotNull(accSerializer);
        checkNotNull(generatedAggregateFunction);

        final WindowProcessor<?> windowProcessor;
        if (assigner instanceof SliceAssigner) {
            windowProcessor = buildSlicingWindowProcessor();
        } else {
            windowProcessor = buildUnslicingWindowProcessor();
        }
        return new WindowAggOperator<>(windowProcessor);
    }

    @SuppressWarnings("unchecked")
    private SlicingWindowProcessor<Long> buildSlicingWindowProcessor() {

        boolean isGlobalAgg =
                localGeneratedAggregateFunction != null && globalGeneratedAggregateFunction != null;

        RecordsCombiner.Factory combinerFactory;
        if (isGlobalAgg) {
            combinerFactory =
                    new GlobalAggCombiner.Factory(
                            (GeneratedNamespaceAggsHandleFunction<Long>)
                                    localGeneratedAggregateFunction,
                            (GeneratedNamespaceAggsHandleFunction<Long>)
                                    globalGeneratedAggregateFunction);
        } else {
            combinerFactory =
                    new AggCombiner.Factory(
                            (GeneratedNamespaceAggsHandleFunction<Long>)
                                    generatedAggregateFunction);
        }
        final WindowBuffer.Factory bufferFactory =
                new RecordsWindowBuffer.Factory(keySerializer, inputSerializer, combinerFactory);

        final SlicingWindowProcessor<Long> windowProcessor;
        if (assigner instanceof SliceSharedAssigner) {
            windowProcessor =
                    new SliceSharedWindowAggProcessor(
                            (GeneratedNamespaceAggsHandleFunction<Long>) generatedAggregateFunction,
                            bufferFactory,
                            (SliceSharedAssigner) assigner,
                            accSerializer,
                            indexOfCountStart,
                            shiftTimeZone);
        } else if (assigner instanceof SliceUnsharedAssigner) {
            windowProcessor =
                    new SliceUnsharedWindowAggProcessor(
                            (GeneratedNamespaceAggsHandleFunction<Long>) generatedAggregateFunction,
                            bufferFactory,
                            (SliceUnsharedAssigner) assigner,
                            accSerializer,
                            indexOfCountStart,
                            shiftTimeZone);
        } else {
            throw new IllegalArgumentException(
                    "assigner must be instance of SliceUnsharedAssigner or SliceSharedAssigner.");
        }
        return windowProcessor;
    }

    @SuppressWarnings("unchecked")
    private UnsliceWindowAggProcessor buildUnslicingWindowProcessor() {
        return new UnsliceWindowAggProcessor(
                (GeneratedNamespaceAggsHandleFunction<TimeWindow>) generatedAggregateFunction,
                (UnsliceAssigner<TimeWindow>) assigner,
                accSerializer,
                indexOfCountStart,
                shiftTimeZone);
    }
}
