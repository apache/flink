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

    public static final long ACTIVE = Long.MAX_VALUE;
    private static final long UNDEFINED = Long.MIN_VALUE;

    private final OperatorIOMetricGroup operatorIOMetricGroup;
    private final Clock clock;
    private final Counter numRecordsInErrors;
    private long lastWatermark = UNDEFINED;
    private long lastEventTime = UNDEFINED;
    private long idleStartTime = ACTIVE;

    private InternalSourceReaderMetricGroup(
            MetricGroup parentMetricGroup,
            OperatorIOMetricGroup operatorIOMetricGroup,
            Clock clock) {
        super(parentMetricGroup);
        numRecordsInErrors = parentMetricGroup.counter(MetricNames.NUM_RECORDS_IN_ERRORS);
        this.operatorIOMetricGroup = operatorIOMetricGroup;
        this.clock = clock;
        parentMetricGroup.gauge(
                MetricNames.SOURCE_IDLE_TIME,
                () -> isIdling() ? this.clock.absoluteTimeMillis() - idleStartTime : 0);
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

    private boolean isIdling() {
        return idleStartTime != ACTIVE;
    }

    public void idlingStarted() {
        if (!isIdling()) {
            idleStartTime = clock.absoluteTimeMillis();
        }
    }

    public void recordEmitted() {
        idleStartTime = ACTIVE;
    }

    @Override
    public Counter getNumRecordsInErrorsCounter() {
        return numRecordsInErrors;
    }

    public void watermarkEmitted(long watermark) {
        // iff a respective source emits a watermark, Flink can provide the watermark lag
        if (lastWatermark == UNDEFINED) {
            lastWatermark = watermark;
            parentMetricGroup.gauge(
                    MetricNames.WATERMARK_LAG, () -> clock.absoluteTimeMillis() - lastWatermark);
        } else {
            lastWatermark = watermark;
        }
    }

    public void eventTimeEmitted(long timestamp) {
        // iff a respective source emits a timestamp, Flink can provide the event lag
        if (lastEventTime == UNDEFINED) {
            lastEventTime = timestamp;
            parentMetricGroup.gauge(
                    MetricNames.CURRENT_EMIT_EVENT_TIME_LAG,
                    () -> getLastEmitTime() - lastEventTime);
        } else {
            lastEventTime = timestamp;
        }
    }

    /**
     * This is a rough approximation. If the source is busy, we assume that <code>emit time == now()
     * </code>. If it's idling, we just take the time it started idling as the last emit time.
     */
    private long getLastEmitTime() {
        return isIdling() ? idleStartTime : clock.absoluteTimeMillis();
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
}
