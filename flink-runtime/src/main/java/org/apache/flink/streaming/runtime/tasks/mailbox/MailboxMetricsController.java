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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Mailbox metrics controller class. The use of mailbox metrics, in particular scheduling latency
 * measurements that require a {@link TimerService}, induce (cyclic) dependencies between {@link
 * MailboxProcessor} and {@link org.apache.flink.streaming.runtime.tasks.StreamTask}. An instance of
 * this class contains and gives control over these dependencies.
 */
@Internal
public class MailboxMetricsController {
    /** Default timer interval in milliseconds for triggering mailbox latency measurement. */
    public final int defaultLatencyMeasurementInterval = 1000;

    private final Histogram latencyHistogram;
    private final Counter mailCounter;

    @Nullable private TimerService timerService;
    @Nullable private MailboxExecutor mailboxExecutor;
    private int measurementInterval = defaultLatencyMeasurementInterval;
    private boolean started = false;

    /**
     * Creates instance of {@link MailboxMetricsController} with references to metrics provided as
     * parameters.
     *
     * @param latencyHistogram Histogram of mailbox latency measurements.
     * @param mailCounter Counter for number of mails processed.
     */
    public MailboxMetricsController(Histogram latencyHistogram, Counter mailCounter) {
        this.timerService = null;
        this.mailboxExecutor = null;
        this.latencyHistogram = latencyHistogram;
        this.mailCounter = mailCounter;
    }

    /**
     * Sets up latency measurement with required {@link TimerService} and {@link MailboxExecutor}.
     *
     * <p>Note: For each instance, latency measurement can be set up only once.
     *
     * @param timerService {@link TimerService} used for latency measurement.
     * @param mailboxExecutor {@link MailboxExecutor} used for latency measurement.
     */
    public void setupLatencyMeasurement(
            TimerService timerService, MailboxExecutor mailboxExecutor) {
        checkState(
                !isLatencyMeasurementSetup(),
                "latency measurement has already been setup and cannot be setup twice");
        this.timerService = timerService;
        this.mailboxExecutor = mailboxExecutor;
    }

    /**
     * Starts mailbox latency measurement. This requires setup of latency measurement via {@link
     * MailboxMetricsController#setupLatencyMeasurement(TimerService, MailboxExecutor)}. Latency is
     * measured through execution of a mail that is triggered by default in the interval defined by
     * {@link MailboxMetricsController#defaultLatencyMeasurementInterval}.
     *
     * <p>Note: For each instance, latency measurement can be started only once.
     */
    public void startLatencyMeasurement() {
        checkState(!isLatencyMeasurementStarted(), "latency measurement has already been started");
        checkState(
                isLatencyMeasurementSetup(),
                "timer service and mailbox executor must be setup for latency measurement");
        scheduleLatencyMeasurement();
        started = true;
    }

    /**
     * Indicates if latency mesurement has been started.
     *
     * @return True if latency measurement has been started.
     */
    public boolean isLatencyMeasurementStarted() {
        return started;
    }

    /**
     * Indicates if latency measurement has been setup.
     *
     * @return True if latency measurement has been setup.
     */
    public boolean isLatencyMeasurementSetup() {
        return this.timerService != null && this.mailboxExecutor != null;
    }

    /**
     * Gets {@link Counter} for number of mails processed.
     *
     * @return {@link Counter} for number of mails processed.
     */
    public Counter getMailCounter() {
        return this.mailCounter;
    }

    @VisibleForTesting
    public void setLatencyMeasurementInterval(int measurementInterval) {
        this.measurementInterval = measurementInterval;
    }

    @VisibleForTesting
    public void measureMailboxLatency() {
        assert mailboxExecutor != null;
        long startTime = SystemClock.getInstance().relativeTimeMillis();
        mailboxExecutor.execute(
                () -> {
                    long endTime = SystemClock.getInstance().relativeTimeMillis();
                    long latency = endTime - startTime;
                    latencyHistogram.update(latency);
                    scheduleLatencyMeasurement();
                },
                "Measure mailbox latency metric");
    }

    private void scheduleLatencyMeasurement() {
        assert timerService != null;
        timerService.registerTimer(
                timerService.getCurrentProcessingTime() + measurementInterval,
                timestamp -> measureMailboxLatency());
    }
}
