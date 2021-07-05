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

package org.apache.flink.table.runtime.operators.aggregate.window.processors;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners;
import org.apache.flink.table.runtime.operators.window.slicing.SliceSharedAssigner;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An window aggregate processor implementation which works for {@link SliceSharedAssigner}, e.g.
 * hopping windows and cumulative windows.
 */
public final class SliceSharedWindowAggProcessor extends AbstractWindowAggProcessor
        implements SliceSharedAssigner.MergeCallback {
    private static final long serialVersionUID = 1L;

    private final SliceSharedAssigner sliceSharedAssigner;
    private final WindowIsEmptySupplier emptySupplier;
    private final SliceMergeTargetHelper mergeTargetHelper;

    public SliceSharedWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            WindowBuffer.Factory bufferFactory,
            SliceSharedAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        super(genAggsHandler, bufferFactory, sliceAssigner, accSerializer, shiftTimeZone);
        this.sliceSharedAssigner = sliceAssigner;
        this.mergeTargetHelper = new SliceMergeTargetHelper();
        this.emptySupplier = new WindowIsEmptySupplier(indexOfCountStar, sliceAssigner);
    }

    @Override
    public void fireWindow(Long windowEnd) throws Exception {
        sliceSharedAssigner.mergeSlices(windowEnd, this);
        // we have set accumulator in the merge() method
        RowData aggResult = aggregator.getValue(windowEnd);
        if (!isWindowEmpty()) {
            // for hopping windows, the triggered window may be an empty window
            // (see register next window below), for such window, we shouldn't emit it
            collect(aggResult);
        }

        // we should register next window timer here,
        // because slices are shared, maybe no elements arrived for the next slices
        Optional<Long> nextWindowEndOptional =
                sliceSharedAssigner.nextTriggerWindow(windowEnd, emptySupplier);
        if (nextWindowEndOptional.isPresent()) {
            long nextWindowEnd = nextWindowEndOptional.get();
            if (sliceSharedAssigner.isEventTime()) {
                windowTimerService.registerEventTimeWindowTimer(nextWindowEnd);
            } else {
                windowTimerService.registerProcessingTimeWindowTimer(nextWindowEnd);
            }
        }
    }

    @Override
    public void merge(@Nullable Long mergeResult, Iterable<Long> toBeMerged) throws Exception {
        // get base accumulator
        final RowData acc;
        if (mergeResult == null) {
            // null means the merged is not on state, create a new acc
            acc = aggregator.createAccumulators();
        } else {
            RowData stateAcc = windowState.value(mergeResult);
            if (stateAcc == null) {
                acc = aggregator.createAccumulators();
            } else {
                acc = stateAcc;
            }
        }
        // set base accumulator
        aggregator.setAccumulators(mergeResult, acc);

        // merge slice accumulators
        for (Long slice : toBeMerged) {
            RowData sliceAcc = windowState.value(slice);
            if (sliceAcc != null) {
                aggregator.merge(slice, sliceAcc);
            }
        }

        // set merged acc into state if the merged acc is on state
        if (mergeResult != null) {
            windowState.update(mergeResult, aggregator.getAccumulators());
        }
    }

    protected long sliceStateMergeTarget(long sliceToMerge) throws Exception {
        mergeTargetHelper.setMergeTarget(null);
        sliceSharedAssigner.mergeSlices(sliceToMerge, mergeTargetHelper);

        // the mergeTarget might be null, which means the merging happens in memory instead of
        // on state, so the slice state to merge into is itself.
        if (mergeTargetHelper.getMergeTarget() != null) {
            return mergeTargetHelper.getMergeTarget();
        } else {
            return sliceToMerge;
        }
    }

    private boolean isWindowEmpty() {
        if (emptySupplier.indexOfCountStar < 0) {
            // for non-hopping windows, the window is never empty
            return false;
        } else {
            return emptySupplier.get();
        }
    }

    // ------------------------------------------------------------------------------------------

    private final class WindowIsEmptySupplier implements Supplier<Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        private final int indexOfCountStar;

        private WindowIsEmptySupplier(int indexOfCountStar, SliceAssigner assigner) {
            if (assigner instanceof SliceAssigners.HoppingSliceAssigner) {
                checkArgument(
                        indexOfCountStar >= 0,
                        "Hopping window requires a COUNT(*) in the aggregate functions.");
            }
            this.indexOfCountStar = indexOfCountStar;
        }

        @Override
        public Boolean get() {
            if (indexOfCountStar < 0) {
                return false;
            }
            try {
                RowData acc = aggregator.getAccumulators();
                return acc == null || acc.getLong(indexOfCountStar) == 0;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private static final class SliceMergeTargetHelper
            implements SliceSharedAssigner.MergeCallback, Serializable {

        private static final long serialVersionUID = 1L;
        private Long mergeTarget = null;

        @Override
        public void merge(@Nullable Long mergeResult, Iterable<Long> toBeMerged) throws Exception {
            this.mergeTarget = mergeResult;
        }

        public Long getMergeTarget() {
            return mergeTarget;
        }

        public void setMergeTarget(Long mergeTarget) {
            this.mergeTarget = mergeTarget;
        }
    }
}
