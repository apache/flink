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

package org.apache.flink.table.runtime.operators.aggregate.asyncwindow.processors;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.asyncwindow.buffers.AsyncStateWindowBuffer;
import org.apache.flink.table.runtime.operators.window.async.AsyncMergeCallback;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceSharedAssigner;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A window aggregate processor implementation which works for {@link SliceSharedAssigner} with
 * async state api, e.g. hopping windows and cumulative windows.
 */
public final class AsyncStateSliceSharedWindowAggProcessor
        extends AbstractAsyncStateSliceWindowAggProcessor
        implements AsyncMergeCallback<Long, Iterable<Long>> {

    private static final long serialVersionUID = 1L;

    private final SliceSharedAssigner sliceSharedAssigner;

    public AsyncStateSliceSharedWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            AsyncStateWindowBuffer.Factory bufferFactory,
            SliceSharedAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(
                genAggsHandler,
                bufferFactory,
                sliceAssigner,
                accSerializer,
                indexOfCountStar,
                shiftTimeZone);
        this.sliceSharedAssigner = sliceAssigner;
    }

    @Override
    public StateFuture<Void> fireWindow(long timerTimestamp, Long windowEnd) throws Exception {
        return sliceSharedAssigner
                .asyncMergeSlices(windowEnd, this)
                .thenAccept(
                        accAndAggResult -> {
                            if (!emptyChecker.apply(accAndAggResult.f0)) {
                                // if the triggered window is an empty window, we shouldn't emit it
                                collect(
                                        ctx.getAsyncKeyContext().getCurrentKey(),
                                        accAndAggResult.f1);
                            }

                            // we should register next window timer here,
                            // because slices are shared, maybe no elements arrived for the next
                            // slices
                            Optional<Long> nextWindowEndOptional =
                                    sliceSharedAssigner.nextTriggerWindow(
                                            windowEnd, accAndAggResult.f0, emptyChecker);
                            if (nextWindowEndOptional.isPresent()) {
                                long nextWindowEnd = nextWindowEndOptional.get();
                                if (sliceSharedAssigner.isEventTime()) {
                                    windowTimerService.registerEventTimeWindowTimer(nextWindowEnd);
                                } else {
                                    windowTimerService.registerProcessingTimeWindowTimer(
                                            nextWindowEnd);
                                }
                            }
                        });
    }

    @Override
    public StateFuture<Tuple2<RowData, RowData>> asyncMerge(
            @Nullable Long mergeResult, Iterable<Long> toBeMerged, Long resultNamespace)
            throws Exception {
        // get base accumulator
        final StateFuture<RowData> accOfMergeResultFuture;
        if (mergeResult == null) {
            // null means the merged is not on state, create a new acc
            accOfMergeResultFuture =
                    StateFutureUtils.completedFuture(aggregator.createAccumulators());
        } else {
            accOfMergeResultFuture =
                    windowState
                            .asyncValue(mergeResult)
                            .thenApply(
                                    stateAcc -> {
                                        if (stateAcc == null) {
                                            return aggregator.createAccumulators();
                                        } else {
                                            return stateAcc;
                                        }
                                    });
        }

        StateFuture<Collection<Tuple2<Long, RowData>>> allAccOfSlicesToBeMergedFuture =
                collectAccOfSlicesToBeMerged(toBeMerged);

        return accOfMergeResultFuture
                .thenCombine(
                        allAccOfSlicesToBeMergedFuture,
                        (accOfMergeResult, allAccOfSlicesToBeMerged) -> {
                            // set base accumulator
                            aggregator.setAccumulators(mergeResult, accOfMergeResult);

                            // merge slice accumulators
                            for (Tuple2<Long, RowData> sliceAndAcc : allAccOfSlicesToBeMerged) {
                                RowData sliceAcc = sliceAndAcc.f1;
                                if (sliceAcc != null) {
                                    aggregator.merge(sliceAndAcc.f0, sliceAcc);
                                }
                            }

                            return Tuple2.of(
                                    aggregator.getAccumulators(),
                                    aggregator.getValue(resultNamespace));
                        })
                .thenCompose(
                        accAndAggResult -> {
                            // set merged acc into state if the merged acc is on state
                            if (mergeResult != null) {
                                return windowState
                                        .asyncUpdate(mergeResult, accAndAggResult.f0)
                                        .thenApply(VOID -> accAndAggResult);
                            } else {
                                return StateFutureUtils.completedFuture(accAndAggResult);
                            }
                        });
    }

    private StateFuture<Collection<Tuple2<Long, RowData>>> collectAccOfSlicesToBeMerged(
            Iterable<Long> slicesToBeMerged) throws Exception {
        List<StateFuture<Tuple2<Long, RowData>>> futures = new ArrayList<>();
        for (Long slice : slicesToBeMerged) {
            futures.add(windowState.asyncValue(slice).thenApply(acc -> Tuple2.of(slice, acc)));
        }
        return StateFutureUtils.combineAll(futures);
    }

    @Override
    protected StateFuture<Long> sliceStateMergeTarget(long sliceToMerge) throws Exception {
        SliceMergeTargetHelper mergeHelper = new SliceMergeTargetHelper();
        return sliceSharedAssigner
                .asyncMergeSlices(sliceToMerge, mergeHelper)
                .thenApply(
                        VOID -> {
                            // the mergeTarget might be null, which means the merging happens in
                            // memory instead of
                            // on state, so the slice state to merge into is itself.
                            if (mergeHelper.getMergeTarget() != null) {
                                return mergeHelper.getMergeTarget();
                            } else {
                                return sliceToMerge;
                            }
                        });
    }

    // ------------------------------------------------------------------------------------------

    private static final class SliceMergeTargetHelper
            implements AsyncMergeCallback<Long, Iterable<Long>>, Serializable {

        private static final long serialVersionUID = 1L;
        private static final StateFuture<Tuple2<RowData, RowData>> REUSABLE_FUTURE_RESULT =
                StateFutureUtils.completedFuture(null);
        private Long mergeTarget = null;

        @Override
        public StateFuture<Tuple2<RowData, RowData>> asyncMerge(
                @Nullable Long mergeResult, Iterable<Long> toBeMerged, Long resultNamespace) {
            this.mergeTarget = mergeResult;
            return REUSABLE_FUTURE_RESULT;
        }

        public Long getMergeTarget() {
            return mergeTarget;
        }
    }
}
