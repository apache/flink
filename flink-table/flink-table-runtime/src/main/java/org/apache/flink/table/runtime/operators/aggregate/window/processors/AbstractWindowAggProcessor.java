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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerWindowStateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;
import org.apache.flink.table.runtime.operators.window.tvf.state.WindowValueState;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A base class for window aggregate processors. */
public abstract class AbstractWindowAggProcessor<W> implements WindowProcessor<W> {

    private static final long serialVersionUID = 1L;

    protected final GeneratedNamespaceAggsHandleFunction<W> genAggsHandler;

    protected final TypeSerializer<RowData> accSerializer;

    protected final boolean isEventTime;

    protected final ZoneId shiftTimeZone;

    /** The shift timezone is using DayLightSaving time or not. */
    protected final boolean useDayLightSaving;

    protected final WindowIsEmptySupplier emptySupplier;

    // ----------------------------------------------------------------------------------------

    protected transient long currentProgress;

    protected transient WindowProcessor.Context<W> ctx;

    protected transient ClockService clockService;

    protected transient WindowTimerService<W> windowTimerService;

    protected transient NamespaceAggsHandleFunction<W> aggregator;

    /** state schema: [key, window, accumulator]. */
    protected transient WindowValueState<W> windowState;

    protected transient JoinedRowData reuseOutput;

    public AbstractWindowAggProcessor(
            GeneratedNamespaceAggsHandleFunction<W> genAggsHandler,
            WindowAssigner sliceAssigner,
            TypeSerializer<RowData> accSerializer,
            boolean isEventTime,
            int indexOfCountStar,
            ZoneId shiftTimeZone) {
        this.genAggsHandler = genAggsHandler;
        this.accSerializer = accSerializer;
        this.isEventTime = isEventTime;
        this.shiftTimeZone = shiftTimeZone;
        this.useDayLightSaving = TimeZone.getTimeZone(shiftTimeZone).useDaylightTime();
        this.emptySupplier = new WindowIsEmptySupplier(indexOfCountStar, sliceAssigner);
    }

    @Override
    public void open(WindowProcessor.Context<W> context) throws Exception {
        this.ctx = context;
        final TypeSerializer<W> namespaceSerializer = createWindowSerializer();
        ValueState<RowData> state =
                ctx.getKeyedStateBackend()
                        .getOrCreateKeyedState(
                                namespaceSerializer,
                                new ValueStateDescriptor<>("window-aggs", accSerializer));
        this.windowState = new WindowValueState<>((InternalValueState<RowData, W, RowData>) state);
        this.clockService = ClockService.of(ctx.getTimerService());
        this.aggregator =
                genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());
        this.aggregator.open(
                new PerWindowStateDataViewStore(
                        ctx.getKeyedStateBackend(), namespaceSerializer, ctx.getRuntimeContext()));
        this.reuseOutput = new JoinedRowData();
        this.currentProgress = Long.MIN_VALUE;
        this.windowTimerService = getWindowTimerService();
    }

    protected abstract WindowTimerService<W> getWindowTimerService();

    @Override
    public void initializeWatermark(long watermark) {
        if (isEventTime) {
            currentProgress = watermark;
        }
    }

    @Override
    public void close() throws Exception {
        if (aggregator != null) {
            aggregator.close();
        }
    }

    /**
     * Send result to downstream.
     *
     * <p>The {@link org.apache.flink.types.RowKind} of the results is always {@link
     * org.apache.flink.types.RowKind#INSERT}.
     *
     * <p>TODO support early fire / late file to produce changelog result.
     */
    protected void collect(RowData aggResult) {
        reuseOutput.replace(ctx.getKeyedStateBackend().getCurrentKey(), aggResult);
        ctx.output(reuseOutput);
    }

    /** A supplier that returns whether the window is empty. */
    protected final class WindowIsEmptySupplier implements Supplier<Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        private final int indexOfCountStar;

        private WindowIsEmptySupplier(int indexOfCountStar, WindowAssigner assigner) {
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
}
