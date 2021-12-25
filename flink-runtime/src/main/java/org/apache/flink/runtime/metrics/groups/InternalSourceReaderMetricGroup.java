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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

/** Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator. */
@Internal
public class InternalSourceReaderMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SourceReaderMetricGroup {
    public static final long UNDEFINED = -1L;
    private static final long ACTIVE = Long.MAX_VALUE;
    private static final long MAX_WATERMARK_TIMESTAMP = Watermark.MAX_WATERMARK.getTimestamp();

    private final OperatorIOMetricGroup operatorIOMetricGroup;
    private final Counter numRecordsInErrors;
    private final Clock clock;
    private long lastWatermark;
    private long lastEventTime = TimestampAssigner.NO_TIMESTAMP;
    private long idleStartTime = ACTIVE;
    private boolean firstWatermark = true;

    private InternalSourceReaderMetricGroup(
            MetricGroup parentMetricGroup,
            OperatorIOMetricGroup operatorIOMetricGroup,
            Clock clock) {
        super(parentMetricGroup);
        numRecordsInErrors = parentMetricGroup.counter(MetricNames.NUM_RECORDS_IN_ERRORS);
        this.operatorIOMetricGroup = operatorIOMetricGroup;
        this.clock = clock;
        parentMetricGroup.gauge(MetricNames.SOURCE_IDLE_TIME, this::getIdleTime);
        parentMetricGroup.gauge(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG, this::getEmitTimeLag);
    }

    public static InternalSourceReaderMetricGroup wrap(OperatorMetricGroup operatorMetricGroup) {
        return new InternalSourceReaderMetricGroup(
                operatorMetricGroup,
                operatorMetricGroup.getIOMetricGroup(),
                SystemClock.getInstance());
    }

    @VisibleForTesting
    public static InternalSourceReaderMetricGroup mock(MetricGroup metricGroup) {
        return new InternalSourceReaderMetricGroup(
                metricGroup,
                UnregisteredMetricsGroup.createOperatorIOMetricGroup(),
                SystemClock.getInstance());
    }

    @Override
    public Counter getNumRecordsInErrorsCounter() {
        return numRecordsInErrors;
    }

    @Override
    public void setPendingBytesGauge(Gauge<Long> pendingBytesGauge) {
        gauge(MetricNames.PENDING_BYTES, pendingBytesGauge);
    }

    @Override
    public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {
        gauge(MetricNames.PENDING_RECORDS, pendingRecordsGauge);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return operatorIOMetricGroup;
    }

    /**
     * Called when a new record was emitted with the given timestamp. {@link
     * TimestampAssigner#NO_TIMESTAMP} should be indicated that the record did not have a timestamp.
     *
     * <p>Note this function should be called before the actual record is emitted such that chained
     * processing does not influence the statistics.
     */
    public void recordEmitted(long timestamp) {
        idleStartTime = ACTIVE;
        lastEventTime = timestamp;
    }

    public void idlingStarted() {
        if (!isIdling()) {
            idleStartTime = clock.absoluteTimeMillis();
        }
    }

    /**
     * Called when a watermark was emitted.
     *
     * <p>Note this function should be called before the actual watermark is emitted such that
     * chained processing does not influence the statistics.
     */
    public void watermarkEmitted(long watermark) {
        if (watermark == MAX_WATERMARK_TIMESTAMP) {
            return;
        }
        lastWatermark = watermark;
        if (firstWatermark) {
            parentMetricGroup.gauge(MetricNames.WATERMARK_LAG, this::getWatermarkLag);
            firstWatermark = false;
        }
    }

    boolean isIdling() {
        return idleStartTime != ACTIVE;
    }

    long getIdleTime() {
        return isIdling() ? this.clock.absoluteTimeMillis() - idleStartTime : 0;
    }

    /**
     * This is a rough approximation. If the source is busy, we assume that <code>
     * emit time == now()
     * </code>. If it's idling, we just take the time it started idling as the last emit time.
     */
    private long getLastEmitTime() {
        return isIdling() ? idleStartTime : clock.absoluteTimeMillis();
    }

    long getEmitTimeLag() {
        return lastEventTime != TimestampAssigner.NO_TIMESTAMP
                ? getLastEmitTime() - lastEventTime
                : UNDEFINED;
    }

    long getWatermarkLag() {
        return getLastEmitTime() - lastWatermark;
    }
}
