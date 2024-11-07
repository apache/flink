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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProcessingTimeServiceImpl}. */
class ProcessingTimeServiceImplTest {

    private static final Duration TESTING_TIMEOUT = Duration.ofSeconds(10L);

    private SystemProcessingTimeService timerService;

    @BeforeEach
    void setup() {
        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();

        timerService = new SystemProcessingTimeService(errorFuture::complete);
    }

    @AfterEach
    void teardown() {
        timerService.shutdownService();
    }

    @Test
    void testTimerRegistrationAndCancellation()
            throws TimeoutException, InterruptedException, ExecutionException {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);

        // test registerTimer() and cancellation
        Future<?> neverFiredTimer =
                processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
        assertThat(timerService.getNumTasksScheduled()).isOne();
        assertThat(neverFiredTimer.cancel(false)).isTrue();
        assertThat(neverFiredTimer).isDone().isCancelled();

        final CompletableFuture<?> firedTimerFuture = new CompletableFuture<>();
        Future<?> firedTimer =
                processingTimeService.registerTimer(
                        0, timestamp -> firedTimerFuture.complete(null));
        firedTimer.get(TESTING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(firedTimerFuture).isDone();
        assertThat(firedTimer).isNotCancelled();

        // test scheduleAtFixedRate() and cancellation
        final CompletableFuture<?> periodicTimerFuture = new CompletableFuture<>();
        Future<?> periodicTimer =
                processingTimeService.scheduleAtFixedRate(
                        timestamp -> periodicTimerFuture.complete(null), 0, Long.MAX_VALUE);

        periodicTimerFuture.get(TESTING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(periodicTimer.cancel(false)).isTrue();
        assertThat(periodicTimer).isDone().isCancelled();
    }

    @Test
    void testQuiesce() throws Exception {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);

        final CompletableFuture<?> timerRunFuture = new CompletableFuture();
        final OneShotLatch timerWaitLatch = new OneShotLatch();

        ScheduledFuture<?> timer =
                processingTimeService.registerTimer(
                        0,
                        timestamp -> {
                            timerRunFuture.complete(null);
                            timerWaitLatch.await();
                        });

        // wait for the timer to run, then quiesce the time service
        timerRunFuture.get(TESTING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CompletableFuture<?> quiesceCompletedFuture = processingTimeService.quiesce();

        // after the timer server is quiesced, tests #registerTimer() and #scheduleAtFixedRate()
        assertThat((Future<?>) processingTimeService.registerTimer(0, timestamp -> {}))
                .isInstanceOf(NeverCompleteFuture.class);
        assertThat(
                        (Future<?>)
                                processingTimeService.scheduleAtFixedRate(
                                        timestamp -> {}, 0, Long.MAX_VALUE))
                .isInstanceOf(NeverCompleteFuture.class);

        // when the timer is finished, the quiesce-completed future should be completed
        assertThat(quiesceCompletedFuture).isNotDone();
        timerWaitLatch.trigger();
        timer.get(TESTING_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(quiesceCompletedFuture).isDone();
    }

    @Test
    void testQuiesceWhenNoRunningTimers() {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);
        assertThat(processingTimeService.quiesce()).isDone();
    }
}
