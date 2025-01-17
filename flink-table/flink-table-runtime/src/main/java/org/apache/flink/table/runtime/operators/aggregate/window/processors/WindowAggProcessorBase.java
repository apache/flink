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
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.tvf.common.ClockService;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowTimerService;

import java.time.ZoneId;
import java.util.TimeZone;

public abstract class WindowAggProcessorBase<W, C extends WindowProcessor.Context<W>>
        implements WindowProcessor<W, C> {

    private static final long serialVersionUID = 1L;

    protected final GeneratedNamespaceAggsHandleFunction<W> genAggsHandler;

    protected final TypeSerializer<RowData> accSerializer;

    protected final boolean isEventTime;

    protected final ZoneId shiftTimeZone;

    /** The shift timezone is using DayLightSaving time or not. */
    protected final boolean useDayLightSaving;

    // ----------------------------------------------------------------------------------------

    protected transient long currentProgress;

    protected transient C ctx;

    protected transient ClockService clockService;

    protected transient WindowTimerService<W> windowTimerService;

    protected transient NamespaceAggsHandleFunction<W> aggregator;

    protected transient JoinedRowData reuseOutput;

    public WindowAggProcessorBase(
            GeneratedNamespaceAggsHandleFunction<W> genAggsHandler,
            TypeSerializer<RowData> accSerializer,
            boolean isEventTime,
            ZoneId shiftTimeZone) {
        this.genAggsHandler = genAggsHandler;
        this.accSerializer = accSerializer;
        this.isEventTime = isEventTime;
        this.shiftTimeZone = shiftTimeZone;
        this.useDayLightSaving = TimeZone.getTimeZone(shiftTimeZone).useDaylightTime();
    }

    @Override
    public void open(C context) throws Exception {
        this.ctx = context;
        this.clockService = ClockService.of(ctx.getTimerService());

        this.aggregator =
                genAggsHandler.newInstance(ctx.getRuntimeContext().getUserCodeClassLoader());

        this.reuseOutput = new JoinedRowData();
        this.currentProgress = Long.MIN_VALUE;
        this.windowTimerService = getWindowTimerService();

        prepareAggregator();
    }

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

    protected abstract void prepareAggregator() throws Exception;

    protected abstract WindowTimerService<W> getWindowTimerService();

    /**
     * Send result to downstream.
     *
     * <p>The {@link org.apache.flink.types.RowKind} of the results is always {@link
     * org.apache.flink.types.RowKind#INSERT}.
     *
     * <p>TODO support early fire / late file to produce changelog result.
     */
    protected void collect(RowData currentKey, RowData aggResult) {
        reuseOutput.replace(currentKey, aggResult);
        ctx.output(reuseOutput);
    }
}
