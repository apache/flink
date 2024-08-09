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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that extracts timestamps from stream elements and generates periodic
 * watermarks.
 */
public class WatermarkAssignerOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>,
                org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final int rowtimeFieldIndex;

    private final long idleTimeout;

    private final WatermarkGenerator watermarkGenerator;

    private transient long lastWatermark;

    private transient long watermarkInterval;

    private transient long timerInterval;

    private transient long currentWatermark;

    // Last time watermark have been (periodically) emitted
    private transient long lastWatermarkPeriodicEmitTime;

    // Last time idleness status has been checked
    private transient long timeSinceLastIdleCheck;

    private transient WatermarkStatus currentStatus = WatermarkStatus.ACTIVE;

    private transient long processedElements;

    private transient long lastIdleCheckProcessedElements = -1;

    /** {@link PausableRelativeClock} that will be paused in case of backpressure. */
    private transient PausableRelativeClock inputActivityClock;

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
        inputActivityClock = new PausableRelativeClock(getProcessingTimeService().getClock());
        getContainingTask()
                .getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .registerBackPressureListener(inputActivityClock);

        // watermark and timestamp should start from 0
        this.currentWatermark = 0;
        this.watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        long now = getProcessingTimeService().getCurrentProcessingTime();
        this.lastWatermarkPeriodicEmitTime = now;
        this.timeSinceLastIdleCheck = now;

        if (watermarkInterval > 0 || idleTimeout > 0) {
            this.timerInterval =
                    calculateProcessingTimeTimerInterval(watermarkInterval, idleTimeout);
            getProcessingTimeService().registerTimer(now + timerInterval, this);
        }

        FunctionUtils.setFunctionRuntimeContext(watermarkGenerator, getRuntimeContext());
        FunctionUtils.openFunction(watermarkGenerator, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        processedElements++;

        if (isIdlenessEnabled() && currentStatus.equals(WatermarkStatus.IDLE)) {
            // mark the channel active
            emitWatermarkStatus(WatermarkStatus.ACTIVE);
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
        // timestamp and now can be off in case TM is heavily overloaded.
        // now and inputActivityNow are using different clocks and can have very different values.
        long now = getProcessingTimeService().getCurrentProcessingTime();
        long inputActivityNow = inputActivityClock.relativeTimeMillis();

        if (watermarkInterval > 0 && lastWatermarkPeriodicEmitTime + watermarkInterval <= now) {
            lastWatermarkPeriodicEmitTime = now;
            advanceWatermark();
        }

        if (processedElements != lastIdleCheckProcessedElements) {
            timeSinceLastIdleCheck = inputActivityNow;
            lastIdleCheckProcessedElements = processedElements;
        }

        if (isIdlenessEnabled()
                && currentStatus.equals(WatermarkStatus.ACTIVE)
                && timeSinceLastIdleCheck + idleTimeout <= inputActivityNow) {
            // mark the channel as idle to ignore watermarks from this channel
            emitWatermarkStatus(WatermarkStatus.IDLE);
        }

        // register next timer
        getProcessingTimeService().registerTimer(now + timerInterval, this);
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
            if (isIdlenessEnabled() && currentStatus.equals(WatermarkStatus.IDLE)) {
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
        getContainingTask()
                .getEnvironment()
                .getMetricGroup()
                .getIOMetricGroup()
                .unregisterBackPressureListener(inputActivityClock);
        FunctionUtils.closeFunction(watermarkGenerator);
        super.close();
    }

    private boolean isIdlenessEnabled() {
        return idleTimeout > 0;
    }

    @VisibleForTesting
    static long calculateProcessingTimeTimerInterval(long watermarkInterval, long idleTimeout) {
        checkArgument(watermarkInterval > 0 || idleTimeout > 0);
        if (watermarkInterval <= 0) {
            return idleTimeout;
        }
        if (idleTimeout <= 0) {
            return watermarkInterval;
        }

        long smallerInterval = Math.min(watermarkInterval, idleTimeout);
        long largerInterval = Math.max(watermarkInterval, idleTimeout);

        // If one of the intervals is 5x smaller, just pick the smaller one. The firing interval
        // for the smaller one this way will be perfectly accurate, while for the larger one it will
        // be good enough™. For example one timer is every 2s the other every 11s, the 2nd timer
        // will be effectively checked every 12s, which is an acceptable accuracy.
        long timerInterval;
        if (smallerInterval * 5 < largerInterval) {
            timerInterval = smallerInterval;
        } else {
            // Otherwise, just pick an interval 5x smaller than the smaller interval. Again accuracy
            // will be good enough™.
            timerInterval = smallerInterval / 5;
        }
        return Math.max(timerInterval, 1);
    }
}
