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
import org.apache.flink.metrics.SettableGauge;
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
    private final OperatorIOMetricGroup operatorIOMetricGroup;
    private final Clock clock;
    private final Counter numRecordsInErrors;
    // only if source emits at least one watermark
    private SettableGauge<Long> watermarkGauge;
    // only if records with event timestamp are emitted
    private SettableGauge<Long> eventTimeGauge;
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
                MetricNames.SOURCE_IDLE_TIME_GAUGE,
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
                UnregisteredMetricsGroup.createUnregisteredOperatorIOMetricGroup(),
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
        if (watermarkGauge == null) {
            watermarkGauge = new SettableGauge<>(watermark);
            parentMetricGroup.gauge(
                    MetricNames.WATERMARK_LAG,
                    () -> clock.absoluteTimeMillis() - watermarkGauge.getValue());
        } else {
            watermarkGauge.setValue(watermark);
        }
    }

    public void eventTimeEmitted(long timestamp) {
        // iff a respective source emits a timestamp, Flink can provide the event lag
        if (eventTimeGauge == null) {
            eventTimeGauge = new SettableGauge<>(timestamp);
            parentMetricGroup.gauge(
                    MetricNames.CURRENT_EMIT_EVENT_TIME_LAG,
                    () -> getLastEmitTime() - eventTimeGauge.getValue());
        } else {
            eventTimeGauge.setValue(timestamp);
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
    public <G extends Gauge<Long>> G setPendingBytesGauge(G pendingBytesGauge) {
        return gauge(MetricNames.PENDING_BYTES_GAUGE, pendingBytesGauge);
    }

    @Override
    public <G extends Gauge<Long>> G setPendingRecordsGauge(G pendingRecordsGauge) {
        return gauge(MetricNames.PENDING_RECORDS_GAUGE, pendingRecordsGauge);
    }

    @Override
    public OperatorIOMetricGroup getIOMetricGroup() {
        return operatorIOMetricGroup;
    }
}
