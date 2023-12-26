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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.metrics.MetricNames.INITIALIZATION_TIME;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class TaskIOMetricGroup extends ProxyMetricGroup<TaskMetricGroup> {
    private static final long INVALID_TIMESTAMP = -1L;

    private final Clock clock;

    private final Counter numBytesIn;
    private final Counter numBytesOut;
    private final SumCounter numRecordsIn;
    private final SumCounter numRecordsOut;
    private final Counter numBuffersOut;
    private final Counter numMailsProcessed;

    private final Meter numBytesInRate;
    private final Meter numBytesOutRate;
    private final Meter numRecordsInRate;
    private final Meter numRecordsOutRate;
    private final Meter numBuffersOutRate;
    private final TimerGauge idleTimePerSecond;
    private final Gauge<Double> busyTimePerSecond;
    private final Gauge<Long> backPressuredTimePerSecond;
    private final TimerGauge softBackPressuredTimePerSecond;
    private final TimerGauge hardBackPressuredTimePerSecond;
    private final TimerGauge changelogBusyTimeMsPerSecond;
    private final Gauge<Long> maxSoftBackPressuredTime;
    private final Gauge<Long> maxHardBackPressuredTime;
    private final Gauge<Long> accumulatedBackPressuredTime;
    private final Gauge<Long> accumulatedIdleTime;
    private final Gauge<Double> accumulatedBusyTime;
    private final Meter mailboxThroughput;
    private final Histogram mailboxLatency;
    private final SizeGauge mailboxSize;
    private final Counter initializationDuration;

    private volatile boolean busyTimeEnabled;

    private long taskStartTime;
    private long taskInitializeTime;

    private final Map<IntermediateResultPartitionID, ResultPartitionBytesCounter>
            resultPartitionBytes = new HashMap<>();

    public TaskIOMetricGroup(TaskMetricGroup parent) {
        this(parent, SystemClock.getInstance());
    }

    public TaskIOMetricGroup(TaskMetricGroup parent, Clock clock) {
        super(parent);
        this.clock = clock;
        this.numBytesIn = counter(MetricNames.IO_NUM_BYTES_IN);
        this.numBytesOut = counter(MetricNames.IO_NUM_BYTES_OUT);
        this.numBytesInRate = meter(MetricNames.IO_NUM_BYTES_IN_RATE, new MeterView(numBytesIn));
        this.numBytesOutRate = meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut));

        this.numRecordsIn = counter(MetricNames.IO_NUM_RECORDS_IN, new SumCounter());
        this.numRecordsOut = counter(MetricNames.IO_NUM_RECORDS_OUT, new SumCounter());
        this.numRecordsInRate =
                meter(MetricNames.IO_NUM_RECORDS_IN_RATE, new MeterView(numRecordsIn));
        this.numRecordsOutRate =
                meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut));

        this.numBuffersOut = counter(MetricNames.IO_NUM_BUFFERS_OUT);
        this.numBuffersOutRate =
                meter(MetricNames.IO_NUM_BUFFERS_OUT_RATE, new MeterView(numBuffersOut));

        this.idleTimePerSecond = gauge(MetricNames.TASK_IDLE_TIME, new TimerGauge(clock));
        this.softBackPressuredTimePerSecond =
                gauge(MetricNames.TASK_SOFT_BACK_PRESSURED_TIME, new TimerGauge(clock));
        this.hardBackPressuredTimePerSecond =
                gauge(MetricNames.TASK_HARD_BACK_PRESSURED_TIME, new TimerGauge(clock));
        this.backPressuredTimePerSecond =
                gauge(MetricNames.TASK_BACK_PRESSURED_TIME, this::getBackPressuredTimeMsPerSecond);

        this.maxSoftBackPressuredTime =
                gauge(
                        MetricNames.TASK_MAX_SOFT_BACK_PRESSURED_TIME,
                        softBackPressuredTimePerSecond::getMaxSingleMeasurement);
        this.maxHardBackPressuredTime =
                gauge(
                        MetricNames.TASK_MAX_HARD_BACK_PRESSURED_TIME,
                        hardBackPressuredTimePerSecond::getMaxSingleMeasurement);

        this.busyTimePerSecond = gauge(MetricNames.TASK_BUSY_TIME, this::getBusyTimePerSecond);

        this.changelogBusyTimeMsPerSecond =
                gauge(MetricNames.CHANGELOG_BUSY_TIME, new TimerGauge(clock));

        this.accumulatedBusyTime =
                gauge(MetricNames.ACC_TASK_BUSY_TIME, this::getAccumulatedBusyTime);
        this.accumulatedBackPressuredTime =
                gauge(
                        MetricNames.ACC_TASK_BACK_PRESSURED_TIME,
                        this::getAccumulatedBackPressuredTimeMs);
        this.accumulatedIdleTime =
                gauge(MetricNames.ACC_TASK_IDLE_TIME, idleTimePerSecond::getAccumulatedCount);

        this.numMailsProcessed = new SimpleCounter();
        this.mailboxThroughput =
                meter(MetricNames.MAILBOX_THROUGHPUT, new MeterView(numMailsProcessed));
        this.mailboxLatency =
                histogram(MetricNames.MAILBOX_LATENCY, new DescriptiveStatisticsHistogram(60));
        this.mailboxSize = gauge(MetricNames.MAILBOX_SIZE, new SizeGauge());
        this.initializationDuration =
                counter(
                        INITIALIZATION_TIME,
                        new Counter() {
                            @Override
                            public void inc() {}

                            @Override
                            public void inc(long n) {}

                            @Override
                            public void dec() {}

                            @Override
                            public void dec(long n) {}

                            @Override
                            public long getCount() {
                                return getTaskInitializationDuration();
                            }
                        });
        this.taskStartTime = INVALID_TIMESTAMP;
        this.taskInitializeTime = INVALID_TIMESTAMP;
    }

    public IOMetrics createSnapshot() {
        return new IOMetrics(
                numRecordsInRate,
                numRecordsOutRate,
                numBytesInRate,
                numBytesOutRate,
                accumulatedBackPressuredTime,
                accumulatedIdleTime,
                accumulatedBusyTime,
                resultPartitionBytes);
    }

    // ============================================================================================
    // Getters
    // ============================================================================================

    public Counter getNumBytesInCounter() {
        return numBytesIn;
    }

    public Counter getNumBytesOutCounter() {
        return numBytesOut;
    }

    public Counter getNumRecordsInCounter() {
        return numRecordsIn;
    }

    public Counter getNumRecordsOutCounter() {
        return numRecordsOut;
    }

    public Counter getNumBuffersOutCounter() {
        return numBuffersOut;
    }

    public Counter getNumMailsProcessedCounter() {
        return numMailsProcessed;
    }

    public TimerGauge getIdleTimeMsPerSecond() {
        return idleTimePerSecond;
    }

    public TimerGauge getSoftBackPressuredTimePerSecond() {
        return softBackPressuredTimePerSecond;
    }

    public TimerGauge getHardBackPressuredTimePerSecond() {
        return hardBackPressuredTimePerSecond;
    }

    public TimerGauge getChangelogBusyTimeMsPerSecond() {
        return changelogBusyTimeMsPerSecond;
    }

    public long getBackPressuredTimeMsPerSecond() {
        return getSoftBackPressuredTimePerSecond().getValue()
                + getHardBackPressuredTimePerSecond().getValue();
    }

    public long getAccumulatedBackPressuredTimeMs() {
        return getSoftBackPressuredTimePerSecond().getAccumulatedCount()
                + getHardBackPressuredTimePerSecond().getAccumulatedCount();
    }

    public void markTaskStart() {
        this.taskStartTime = clock.absoluteTimeMillis();
    }

    public void markTaskInitializationStarted() {
        if (taskInitializeTime == INVALID_TIMESTAMP) {
            this.taskInitializeTime = clock.absoluteTimeMillis();
        }
    }

    /**
     * Returns the duration of time required for a task's restoring/initialization, which reaches
     * its maximum when the task begins running and remains constant throughout the task's running.
     * Return 0 when the task is not in initialization/running status.
     */
    @VisibleForTesting
    public long getTaskInitializationDuration() {
        if (taskInitializeTime == INVALID_TIMESTAMP) {
            return 0L;
        } else if (taskStartTime == INVALID_TIMESTAMP) {
            return clock.absoluteTimeMillis() - taskInitializeTime;
        } else {
            return taskStartTime - taskInitializeTime;
        }
    }

    public void setEnableBusyTime(boolean enabled) {
        busyTimeEnabled = enabled;
    }

    @VisibleForTesting
    double getBusyTimePerSecond() {
        double busyTime = idleTimePerSecond.getValue() + getBackPressuredTimeMsPerSecond();
        return busyTimeEnabled ? 1000.0 - Math.min(busyTime, 1000.0) : Double.NaN;
    }

    @VisibleForTesting
    double getAccumulatedBusyTime() {
        if (!busyTimeEnabled) {
            return Double.NaN;
        }
        if (taskStartTime == INVALID_TIMESTAMP) {
            return Double.NaN;
        } else {
            return Math.max(
                    clock.absoluteTimeMillis()
                            - taskStartTime
                            - idleTimePerSecond.getAccumulatedCount()
                            - getAccumulatedBackPressuredTimeMs(),
                    0);
        }
    }

    public Meter getMailboxThroughput() {
        return mailboxThroughput;
    }

    public Histogram getMailboxLatency() {
        return mailboxLatency;
    }

    public Gauge<Integer> getMailboxSize() {
        return mailboxSize;
    }

    // ============================================================================================
    // Metric Reuse
    // ============================================================================================
    public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
        this.numRecordsIn.addCounter(numRecordsInCounter);
    }

    public void reuseRecordsOutputCounter(Counter numRecordsOutCounter) {
        this.numRecordsOut.addCounter(numRecordsOutCounter);
    }

    public void registerResultPartitionBytesCounter(
            IntermediateResultPartitionID resultPartitionId,
            ResultPartitionBytesCounter resultPartitionBytesCounter) {
        this.resultPartitionBytes.put(resultPartitionId, resultPartitionBytesCounter);
    }

    public void registerMailboxSizeSupplier(SizeSupplier<Integer> supplier) {
        this.mailboxSize.registerSupplier(supplier);
    }

    /**
     * A {@link SimpleCounter} that can contain other {@link Counter}s. A call to {@link
     * SumCounter#getCount()} returns the sum of this counters and all contained counters.
     */
    private static class SumCounter extends SimpleCounter {
        private final List<Counter> internalCounters = new ArrayList<>();

        SumCounter() {}

        public void addCounter(Counter toAdd) {
            internalCounters.add(toAdd);
        }

        @Override
        public long getCount() {
            long sum = super.getCount();
            for (Counter counter : internalCounters) {
                sum += counter.getCount();
            }
            return sum;
        }
    }

    private static class SizeGauge implements Gauge<Integer> {
        private SizeSupplier<Integer> supplier;

        public void registerSupplier(SizeSupplier<Integer> supplier) {
            this.supplier = supplier;
        }

        @Override
        public Integer getValue() {
            if (supplier != null) {
                return supplier.get();
            } else {
                return 0; // return "assumed" empty queue size
            }
        }
    }

    /** Supplier for sizes. */
    @FunctionalInterface
    public interface SizeSupplier<R> {
        R get();
    }
}
