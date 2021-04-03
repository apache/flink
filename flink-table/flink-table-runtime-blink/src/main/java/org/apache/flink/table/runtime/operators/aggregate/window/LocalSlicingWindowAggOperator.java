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
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.WindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.LocalAggRecordsCombiner;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.combines.WindowCombineFunction;
import org.apache.flink.table.runtime.operators.window.slicing.ClockService;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.util.TimeWindowUtil;

import java.time.ZoneId;

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
    private final WindowBuffer.Factory windowBufferFactory;
    private final WindowCombineFunction.LocalFactory combinerFactory;

    /**
     * The shift timezone of the window, if the proctime or rowtime type is TIMESTAMP_LTZ, the shift
     * timezone is the timezone user configured in TableConfig, other cases the timezone is UTC
     * which means never shift when assigning windows.
     */
    private final ZoneId shiftTimezone;

    // ------------------------------------------------------------------------

    /** This is used for emitting elements with a given timestamp. */
    protected transient TimestampedCollector<RowData> collector;

    /** Flag to prevent duplicate function.close() calls in close() and dispose(). */
    private transient boolean functionsClosed = false;

    /** current watermark of this operator, the value has considered shifted timezone. */
    private transient long currentWatermark;

    /** The next watermark to trigger windows, the value has considered shifted timezone. */
    private transient long nextTriggerWatermark;

    /** A buffer to buffers window data in memory and may flush to output. */
    private transient WindowBuffer windowBuffer;

    public LocalSlicingWindowAggOperator(
            RowDataKeySelector keySelector,
            SliceAssigner sliceAssigner,
            PagedTypeSerializer<RowData> keySer,
            AbstractRowDataSerializer<RowData> inputSer,
            GeneratedNamespaceAggsHandleFunction<Long> genAggsHandler,
            ZoneId shiftTimezone) {
        this(
                keySelector,
                sliceAssigner,
                new RecordsWindowBuffer.Factory(keySer, inputSer),
                new LocalAggRecordsCombiner.Factory(genAggsHandler, keySer),
                shiftTimezone);
    }

    public LocalSlicingWindowAggOperator(
            RowDataKeySelector keySelector,
            SliceAssigner sliceAssigner,
            WindowBuffer.Factory windowBufferFactory,
            WindowCombineFunction.LocalFactory combinerFactory,
            ZoneId shiftTimezone) {
        this.keySelector = keySelector;
        this.sliceAssigner = sliceAssigner;
        this.windowInterval = sliceAssigner.getSliceEndInterval();
        this.windowBufferFactory = windowBufferFactory;
        this.combinerFactory = combinerFactory;
        this.shiftTimezone = shiftTimezone;
    }

    @Override
    public void open() throws Exception {
        super.open();
        functionsClosed = false;

        collector = new TimestampedCollector<>(output);
        collector.eraseTimestamp();

        final WindowCombineFunction localCombiner =
                combinerFactory.create(getRuntimeContext(), collector);
        this.windowBuffer =
                windowBufferFactory.create(
                        getContainingTask(),
                        getContainingTask().getEnvironment().getMemoryManager(),
                        computeMemorySize(),
                        localCombiner);
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
        long timestamp = TimeWindowUtil.toUtcTimestampMills(mark.getTimestamp(), shiftTimezone);
        if (timestamp > currentWatermark) {
            currentWatermark = timestamp;
            if (currentWatermark >= nextTriggerWatermark) {
                // we only need to call advanceProgress() when currentWatermark may trigger window
                windowBuffer.advanceProgress(currentWatermark);
                nextTriggerWatermark = getNextTriggerWatermark(currentWatermark, windowInterval);
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
        windowBuffer.close();
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

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------
    /** Method to get the next watermark to trigger window. */
    private static long getNextTriggerWatermark(long currentWatermark, long interval) {
        long start = TimeWindow.getWindowStartWithOffset(currentWatermark, 0L, interval);
        long triggerWatermark = start + interval - 1;
        if (triggerWatermark > currentWatermark) {
            return triggerWatermark;
        } else {
            return triggerWatermark + interval;
        }
    }
}
