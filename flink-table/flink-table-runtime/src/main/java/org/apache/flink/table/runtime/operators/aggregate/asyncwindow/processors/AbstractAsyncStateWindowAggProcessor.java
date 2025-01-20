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

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.dataview.UnsupportedStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.WindowAggProcessorBase;
import org.apache.flink.table.runtime.operators.window.async.tvf.common.AsyncStateWindowProcessor;
import org.apache.flink.table.runtime.operators.window.async.tvf.state.WindowAsyncValueState;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A base class for window aggregate processors with async state. */
public abstract class AbstractAsyncStateWindowAggProcessor<W>
        extends WindowAggProcessorBase<W, AsyncStateWindowProcessor.AsyncStateContext<W>>
        implements AsyncStateWindowProcessor<W> {

    private static final long serialVersionUID = 1L;

    protected final WindowIsEmptyChecker emptyChecker;

    private final W defaultWindow;

    // ----------------------------------------------------------------------------------------

    /** state schema: [key, window, accumulator]. */
    protected transient WindowAsyncValueState<W> windowState;

    public AbstractAsyncStateWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<W> genAggsHandler,
            WindowAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            boolean isEventTime,
            int indexOfCountStar,
            ZoneId shiftTimeZone,
            W defaultWindow) {
        super(genAggsHandler, accSerializer, isEventTime, shiftTimeZone);
        this.emptyChecker = new WindowIsEmptyChecker(indexOfCountStar, sliceAssigner);
        this.defaultWindow = defaultWindow;
    }

    @Override
    public void open(AsyncStateContext<W> context) throws Exception {
        super.open(context);

        ValueState<RowData> state =
                ctx.getAsyncKeyContext()
                        .getAsyncKeyedStateBackend()
                        .getOrCreateKeyedState(
                                defaultWindow,
                                createWindowSerializer(),
                                new ValueStateDescriptor<>("window-aggs", accSerializer));
        this.windowState =
                new WindowAsyncValueState<>((InternalValueState<RowData, W, RowData>) state);
    }

    @Override
    protected final void prepareAggregator() throws Exception {
        this.aggregator =
                genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        this.aggregator.open(new UnsupportedStateDataViewStore(ctx.getRuntimeContext()));
    }

    /** A checker that checks whether the window is empty. */
    protected static final class WindowIsEmptyChecker
            implements Function<RowData, Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        private final int indexOfCountStar;

        private WindowIsEmptyChecker(int indexOfCountStar, WindowAssigner assigner) {
            if (assigner instanceof SliceAssigners.HoppingSliceAssigner) {
                checkArgument(
                        indexOfCountStar >= 0,
                        "Hopping window requires a COUNT(*) in the aggregate functions.");
            }
            this.indexOfCountStar = indexOfCountStar;
        }

        @Override
        public Boolean apply(@Nullable RowData acc) {
            if (indexOfCountStar < 0) {
                return false;
            }
            try {
                return acc == null || acc.getLong(indexOfCountStar) == 0;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
