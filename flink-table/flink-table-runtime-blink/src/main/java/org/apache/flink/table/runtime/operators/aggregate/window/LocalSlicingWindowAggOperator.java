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

import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.window.slicing.ClockService;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;

import java.time.ZoneId;
import java.util.TimeZone;

import static org.apache.flink.table.runtime.util.TimeWindowUtil.getNextTriggerWatermark;

/**
 * The operator used for local window aggregation.
 *
 * <p>Note: this only supports event-time window.
 */
public class LocalSlicingWindowAggOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {
    private static final long serialVersionUID = 1L;
    private static final ClockService CLOCK_SERVICE = ClockService.ofSystem();

    private final RowDataKeySelector keySelector;
    private final SliceAssigner sliceAssigner;
    private final long windowInterval;
    private final WindowBuffer.LocalFactory windowBufferFactory;

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    private final ZoneId shiftTimezone;

    /** The shift timezone is using DayLightSaving time or not. */
    private final boolean useDayLightSaving;

    // ------------------------------------------------------------------------

    /** This is used for emitting elements with a given timestamp. */
    protected transient TimestampedCollector<RowData> collector;

    /** Flag to prevent duplicate function.close() calls in close() and dispose(). */
    private transient boolean functionsClosed = false;

    /** current watermark of this operator. */
    private transient long currentWatermark;

    /** The next watermark to trigger windows. */
    private transient long nextTriggerWatermark;

    /** A buffer to buffers window data in memory and may flush to output. */
    private transient WindowBuffer windowBuffer;

    public LocalSlicingWindowAggOperator(
            RowDataKeySelector keySelector,
            SliceAssigner sliceAssigner,
            WindowBuffer.LocalFactory windowBufferFactory,
            ZoneId shiftTimezone) {
        this.keySelector = keySelector;
        this.sliceAssigner = sliceAssigner;
        this.windowInterval = sliceAssigner.getSliceEndInterval();
        this.windowBufferFactory = windowBufferFactory;
        this.shiftTimezone = shiftTimezone;
        this.useDayLightSaving = TimeZone.getTimeZone(shiftTimezone).useDaylightTime();
    }

    @Override
    public void open() throws Exception {
        super.open();
        functionsClosed = false;

        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        this.windowBuffer =
                windowBufferFactory.create(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        computeMemorySize(),
                        getRuntimeContext(),
                        collector,
                        shiftTimezone);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData inputRow = element.getValue();
        RowData key = keySelector.getKey(inputRow);
        long sliceEnd = sliceAssigner.assignSliceEnd(inputRow, CLOCK_SERVICE);
        // may flush to output if buffer is full
        windowBuffer.addElement(key, sliceEnd, inputRow);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() > currentWatermark) {
            currentWatermark = mark.getTimestamp();
            if (currentWatermark >= nextTriggerWatermark) {
                // we only need to call advanceProgress() when current watermark may trigger window
                windowBuffer.advanceProgress(currentWatermark);
                nextTriggerWatermark =
                        getNextTriggerWatermark(
                                currentWatermark, windowInterval, shiftTimezone, useDayLightSaving);
            }
        }
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        windowBuffer.flush();
    }

    @Override
    public void close() throws Exception {
        super.close();
        collector = null;
        functionsClosed = true;
        if (windowBuffer != null) {
            windowBuffer.close();
        }
    }

    @Override
    public void dispose() throws Exception {
        super.dispose();
        collector = null;
        if (!functionsClosed) {
            functionsClosed = true;
            windowBuffer.close();
        }
    }

    /** Compute memory size from memory faction. */
    private long computeMemorySize() {
        final Environment environment = getContainingTask().getEnvironment();
        return environment
                .getMemoryManager()
                .computeMemorySize(
                        getOperatorConfig()
                                .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                        ManagedMemoryUseCase.OPERATOR,
                                        environment.getTaskManagerInfo().getConfiguration(),
                                        environment.getUserCodeClassLoader().asClassLoader()));
    }
}
