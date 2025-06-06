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
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SourceSplitMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Special {@link MetricGroup} representing an {@link SplitEnumerator}. */
@Internal
public class InternalSourceSplitMetricGroup extends ProxyMetricGroup<MetricGroup>
        implements SourceSplitMetricGroup {

    static final Logger LOG = LoggerFactory.getLogger(InternalSourceSplitMetricGroup.class);
    private final TimerGauge pausedTimePerSecond;
    private final TimerGauge idleTimePerSecond;
    private final Gauge<Long> currentWatermarkGauge;
    private final Clock clock;
    private static final String SPLIT = "split";
    private static final String WATERMARK = "watermark";
    private static final long SPLIT_NOT_STARTED = -1L;
    private long splitStartTime = SPLIT_NOT_STARTED;
    private final MetricGroup splitWatermarkMetricGroup;

    private InternalSourceSplitMetricGroup(
            MetricGroup parentMetricGroup,
            Clock clock,
            String splitId,
            Gauge<Long> currentWatermark) {
        super(parentMetricGroup);
        this.clock = clock;
        splitWatermarkMetricGroup = parentMetricGroup.addGroup(SPLIT, splitId).addGroup(WATERMARK);
        pausedTimePerSecond =
                splitWatermarkMetricGroup.gauge(
                        MetricNames.SPLIT_PAUSED_TIME, new TimerGauge(clock));
        idleTimePerSecond =
                splitWatermarkMetricGroup.gauge(MetricNames.SPLIT_IDLE_TIME, new TimerGauge(clock));
        splitWatermarkMetricGroup.gauge(
                MetricNames.SPLIT_ACTIVE_TIME, this::getActiveTimePerSecond);
        splitWatermarkMetricGroup.gauge(
                MetricNames.ACC_SPLIT_PAUSED_TIME, this::getAccumulatedPausedTime);
        splitWatermarkMetricGroup.gauge(
                MetricNames.ACC_SPLIT_ACTIVE_TIME, this::getAccumulatedActiveTime);
        splitWatermarkMetricGroup.gauge(
                MetricNames.ACC_SPLIT_IDLE_TIME, this::getAccumulatedIdleTime);
        currentWatermarkGauge =
                splitWatermarkMetricGroup.gauge(
                        MetricNames.SPLIT_CURRENT_WATERMARK, currentWatermark);
    }

    public static InternalSourceSplitMetricGroup wrap(
            OperatorMetricGroup operatorMetricGroup, String splitId, Gauge<Long> currentWatermark) {
        return new InternalSourceSplitMetricGroup(
                operatorMetricGroup, SystemClock.getInstance(), splitId, currentWatermark);
    }

    @VisibleForTesting
    public static InternalSourceSplitMetricGroup mock(
            MetricGroup metricGroup, String splitId, Gauge<Long> currentWatermark) {
        return new InternalSourceSplitMetricGroup(
                metricGroup, SystemClock.getInstance(), splitId, currentWatermark);
    }

    @VisibleForTesting
    public static InternalSourceSplitMetricGroup wrap(
            OperatorMetricGroup operatorMetricGroup,
            Clock clock,
            String splitId,
            Gauge<Long> currentWatermark) {
        return new InternalSourceSplitMetricGroup(
                operatorMetricGroup, clock, splitId, currentWatermark);
    }

    public void markSplitStart() {
        splitStartTime = clock.absoluteTimeMillis();
    }

    public void maybeMarkSplitStart() {
        if (splitStartTime == SPLIT_NOT_STARTED) {
            markSplitStart();
        }
    }

    public long getCurrentWatermark() {
        return this.currentWatermarkGauge.getValue();
    }

    public void markPaused() {
        maybeMarkSplitStart();
        if (isIdle()) {
            // If a split got paused it means it emitted records,
            // hence it shouldn't be considered idle anymore
            markNotIdle();
            LOG.warn("Split marked paused while still idle");
        }
        this.pausedTimePerSecond.markStart();
    }

    public void markIdle() {
        maybeMarkSplitStart();
        if (isPaused()) {
            // If a split is marked idle, it has no records to emit.
            // hence it shouldn't be considered paused anymore
            markNotPaused();
            LOG.warn("Split marked idle while still paused");
        }
        this.idleTimePerSecond.markStart();
    }

    public void markNotPaused() {
        maybeMarkSplitStart();
        this.pausedTimePerSecond.markEnd();
    }

    public void markNotIdle() {
        maybeMarkSplitStart();
        this.idleTimePerSecond.markEnd();
    }

    public double getActiveTimePerSecond() {
        if (splitStartTime == SPLIT_NOT_STARTED) {
            return 0L;
        }
        double activeTimePerSecond = 1000.0 - getPausedTimePerSecond() - getIdleTimePerSecond();
        return Math.max(activeTimePerSecond, 0);
    }

    public double getAccumulatedActiveTime() {
        if (splitStartTime == SPLIT_NOT_STARTED) {
            return 0L;
        }
        return Math.max(
                clock.absoluteTimeMillis()
                        - splitStartTime
                        - getAccumulatedPausedTime()
                        - getAccumulatedIdleTime(),
                0);
    }

    public long getAccumulatedIdleTime() {
        return idleTimePerSecond.getAccumulatedCount();
    }

    public long getIdleTimePerSecond() {
        return idleTimePerSecond.getValue();
    }

    public long getPausedTimePerSecond() {
        return pausedTimePerSecond.getValue();
    }

    public long getAccumulatedPausedTime() {
        return pausedTimePerSecond.getAccumulatedCount();
    }

    public Boolean isPaused() {
        return pausedTimePerSecond.isMeasuring();
    }

    public Boolean isIdle() {
        return idleTimePerSecond.isMeasuring();
    }

    public Boolean isActive() {
        return !isPaused() && !isIdle();
    }

    public void onSplitFinished() {
        if (splitWatermarkMetricGroup instanceof AbstractMetricGroup) {
            ((AbstractMetricGroup) splitWatermarkMetricGroup).close();
        } else {
            if (splitWatermarkMetricGroup != null) {
                LOG.warn(
                        "Split watermark metric group can not be closed, expecting an instance of AbstractMetricGroup but got: ",
                        splitWatermarkMetricGroup.getClass().getName());
            }
        }
    }

    @VisibleForTesting
    public MetricGroup getSplitWatermarkMetricGroup() {
        return splitWatermarkMetricGroup;
    }
}
