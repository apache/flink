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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that extracts timestamps from stream elements and generates periodic
 * watermarks.
 */
public class WatermarkAssignerOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final int rowtimeFieldIndex;

    private final long idleTimeout;

    private final WatermarkGenerator watermarkGenerator;

    private transient long lastWatermark;

    private transient long watermarkInterval;

    private transient long currentWatermark;

    private transient long lastRecordTime;

    private transient WatermarkStatus currentStatus = WatermarkStatus.ACTIVE;

    /**
     * Create a watermark assigner operator.
     *
     * @param rowtimeFieldIndex the field index to extract event timestamp
     * @param watermarkGenerator the watermark generator
     * @param idleTimeout (idleness checking timeout)
     */
    public WatermarkAssignerOperator(
            int rowtimeFieldIndex,
            WatermarkGenerator watermarkGenerator,
            long idleTimeout,
            ProcessingTimeService processingTimeService) {

        this.rowtimeFieldIndex = rowtimeFieldIndex;
        this.watermarkGenerator = watermarkGenerator;

        this.idleTimeout = idleTimeout;
        this.chainingStrategy = ChainingStrategy.ALWAYS;

        this.processingTimeService = checkNotNull(processingTimeService);
    }

    @Override
    public void open() throws Exception {
        super.open();

        // watermark and timestamp should start from 0
        this.currentWatermark = 0;
        this.watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        this.lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();

        if (watermarkInterval > 0) {
            long now = getProcessingTimeService().getCurrentProcessingTime();
            getProcessingTimeService().registerTimer(now + watermarkInterval, this);
        }

        FunctionUtils.setFunctionRuntimeContext(watermarkGenerator, getRuntimeContext());
        FunctionUtils.openFunction(watermarkGenerator, new Configuration());
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (idleTimeout > 0 && currentStatus.equals(WatermarkStatus.IDLE)) {
            // mark the channel active
            emitWatermarkStatus(WatermarkStatus.ACTIVE);
            lastRecordTime = getProcessingTimeService().getCurrentProcessingTime();
        }
        RowData row = element.getValue();
        if (row.isNullAt(rowtimeFieldIndex)) {
            throw new RuntimeException(
                    "RowTime field should not be null,"
                            + " please convert it to a non-null long value.");
        }
        Long watermark = watermarkGenerator.currentWatermark(row);
        if (watermark != null) {
            currentWatermark = Math.max(currentWatermark, watermark);
        }
        // forward element
        output.collect(element);

        // eagerly emit watermark to avoid period timer not called (this often happens when cpu load
        // is high)
        // current_wm - last_wm > interval
        if (currentWatermark - lastWatermark > watermarkInterval) {
            advanceWatermark();
        }
    }

    private void advanceWatermark() {
        if (currentWatermark > lastWatermark) {
            lastWatermark = currentWatermark;
            // emit watermark
            output.emitWatermark(new Watermark(currentWatermark));
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        advanceWatermark();

        if (idleTimeout > 0 && currentStatus.equals(WatermarkStatus.ACTIVE)) {
            final long currentTime = getProcessingTimeService().getCurrentProcessingTime();
            if (currentTime - lastRecordTime > idleTimeout) {
                // mark the channel as idle to ignore watermarks from this channel
                emitWatermarkStatus(WatermarkStatus.IDLE);
            }
        }

        // register next timer
        long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from upstream (we
     * rely only on the {@link WatermarkGenerator} to emit watermarks from here).
     */
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
            if (idleTimeout > 0 && currentStatus.equals(WatermarkStatus.IDLE)) {
                // mark the channel active
                emitWatermarkStatus(WatermarkStatus.ACTIVE);
            }
            currentWatermark = Long.MAX_VALUE;
            output.emitWatermark(mark);
        }
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        emitWatermarkStatus(watermarkStatus);
    }

    private void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        this.currentStatus = watermarkStatus;
        output.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void finish() throws Exception {
        // all records have been processed, emit a final watermark
        processWatermark(Watermark.MAX_WATERMARK);
    }

    @Override
    public void close() throws Exception {
        FunctionUtils.closeFunction(watermarkGenerator);
        super.close();
    }
}
