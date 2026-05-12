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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link MailboxPartialWatermarkProcessor}. */
class MailboxPartialWatermarkProcessorTest {

    @Test
    void testWatermarkAdvancementWithoutInterruption() throws Exception {
        // Setup: Empty mailbox, single timer service
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final InternalTimerServiceImpl<?, ?> timerService = createMockTimerService(true);
        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(timerService), consumedWatermarks::add);

        // Execute
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify
        verify(timerService).tryAdvanceWatermark(eq(100L), any());
        assertThat(consumedWatermarks).containsExactly(new Watermark(100));
        assertThat(mailbox.hasMail()).isFalse();
    }

    @Test
    void testWatermarkAdvancementWithInterruption() throws Exception {
        // Setup: Timer service that interrupts first time
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final AtomicInteger attemptCount = new AtomicInteger(0);
        final InternalTimerServiceImpl<?, ?> timerService =
                createMockTimerServiceWithAttempts(attemptCount, 2);

        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(timerService), consumedWatermarks::add);

        // Execute
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify: First attempt interrupted, continuation mail scheduled
        assertThat(consumedWatermarks).isEmpty();
        assertThat(mailbox.hasMail()).isTrue();
        assertThat(attemptCount.get()).isEqualTo(1);

        // Execute continuation mail
        while (mailbox.hasMail()) {
            mailbox.take(TaskMailbox.MIN_PRIORITY).run();
        }

        // Verify: Second attempt succeeded
        assertThat(consumedWatermarks).containsExactly(new Watermark(100));
        assertThat(attemptCount.get()).isEqualTo(2);
    }

    @Test
    void testDuplicateSchedulingPrevention() throws Exception {
        // Setup: Timer service that completes after 3 attempts
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final AtomicInteger attemptCount = new AtomicInteger(0);
        final InternalTimerServiceImpl<?, ?> timerService =
                createMockTimerServiceWithAttempts(attemptCount, 3);
        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(timerService), consumedWatermarks::add);

        // Execute twice rapidly
        processor.emitWatermarkInsideMailbox(new Watermark(100));
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify: Both calls attempt to advance, but only one continuation mail scheduled
        assertThat(consumedWatermarks).isEmpty();
        assertThat(attemptCount.get()).isEqualTo(2); // Both calls attempted

        // Execute continuation mails until completion
        while (mailbox.hasMail()) {
            mailbox.take(TaskMailbox.MIN_PRIORITY).run();
        }

        // Verify: Completed after exactly 3 attempts total (2 initial + 1 continuation)
        assertThat(attemptCount.get()).isEqualTo(3);
        assertThat(consumedWatermarks).containsExactly(new Watermark(100));
    }

    @Test
    void testWatermarkMonotonicity() throws Exception {
        // Setup
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final InternalTimerServiceImpl<?, ?> timerService = createMockTimerService(true);
        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(timerService), consumedWatermarks::add);

        // Execute: Non-monotonic watermarks
        processor.emitWatermarkInsideMailbox(new Watermark(100));
        processor.emitWatermarkInsideMailbox(new Watermark(50));
        processor.emitWatermarkInsideMailbox(new Watermark(75));
        processor.emitWatermarkInsideMailbox(new Watermark(200));

        // Verify: maxInputWatermark tracks maximum and timer service called with it
        verify(timerService, times(3)).tryAdvanceWatermark(eq(100L), any());
        verify(timerService, times(1)).tryAdvanceWatermark(eq(200L), any());
        assertThat(consumedWatermarks)
                .containsExactly(
                        new Watermark(100),
                        new Watermark(100), // 50 ignored
                        new Watermark(100), // 75 ignored
                        new Watermark(200));
    }

    @Test
    void testMultipleTimerServices() throws Exception {
        // Setup: Three timer services, second interrupts first
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final InternalTimerServiceImpl<?, ?> service1 = createMockTimerService(true);
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final InternalTimerServiceImpl<?, ?> service2 =
                createMockTimerServiceWithAttempts(attemptCount, 2);
        final InternalTimerServiceImpl<?, ?> service3 = createMockTimerService(true);

        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test",
                        mailboxExecutor,
                        Arrays.asList(service1, service2, service3),
                        consumedWatermarks::add);

        // Execute
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify: service1 called, service2 interrupted
        verify(service1).tryAdvanceWatermark(eq(100L), any());
        verify(service2).tryAdvanceWatermark(eq(100L), any());
        verify(service3, times(0)).tryAdvanceWatermark(anyLong(), any());
        assertThat(consumedWatermarks).isEmpty();

        // Execute continuation
        while (mailbox.hasMail()) {
            mailbox.take(TaskMailbox.MIN_PRIORITY).run();
        }

        // Verify: All services called
        verify(service1, times(2)).tryAdvanceWatermark(eq(100L), any());
        verify(service2, times(2)).tryAdvanceWatermark(eq(100L), any());
        verify(service3).tryAdvanceWatermark(eq(100L), any());
        assertThat(consumedWatermarks).containsExactly(new Watermark(100));
    }

    @Test
    void testEmptyTimerServicesList() throws Exception {
        // Setup: Empty list
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(), consumedWatermarks::add);

        // Execute
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify: Consumer immediately called
        assertThat(consumedWatermarks).containsExactly(new Watermark(100));
        assertThat(mailbox.hasMail()).isFalse();
    }

    @Test
    void testConsumerExceptionPropagation() {
        // Setup: Consumer that throws
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        final InternalTimerServiceImpl<?, ?> timerService = createMockTimerService(true);

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test",
                        mailboxExecutor,
                        List.of(timerService),
                        watermark -> {
                            throw new RuntimeException("Test exception");
                        });

        // Execute and verify exception propagates
        assertThatThrownBy(() -> processor.emitWatermarkInsideMailbox(new Watermark(100)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Test exception");
    }

    @Test
    void testContinuationMailIsDeferrable() throws Exception {
        // Setup: Custom mailbox executor that tracks mail options
        final AtomicBoolean deferrableUsed = new AtomicBoolean(false);
        final List<ThrowingRunnable<? extends Exception>> scheduledMails = new ArrayList<>();
        final MailboxExecutor mailboxExecutor =
                createTrackingMailboxExecutor(deferrableUsed, scheduledMails);

        final InternalTimerServiceImpl<?, ?> timerService = createMockTimerService(false);
        final List<Watermark> consumedWatermarks = new ArrayList<>();

        final MailboxPartialWatermarkProcessor processor =
                new MailboxPartialWatermarkProcessor(
                        "test", mailboxExecutor, List.of(timerService), consumedWatermarks::add);

        // Execute: Should schedule deferrable mail
        processor.emitWatermarkInsideMailbox(new Watermark(100));

        // Verify: Mail was scheduled as deferrable
        assertThat(deferrableUsed.get()).isTrue();
        // The consumer is not called since timer service never completes
        assertThat(consumedWatermarks).isEmpty();
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private MailboxExecutor createTrackingMailboxExecutor(
            AtomicBoolean deferrableUsed,
            List<ThrowingRunnable<? extends Exception>> scheduledMails) {
        return new MailboxExecutor() {
            @Override
            public void execute(
                    MailOptions mailOptions,
                    ThrowingRunnable<? extends Exception> command,
                    String descriptionFormat,
                    Object... descriptionArgs) {
                if (mailOptions == MailOptions.deferrable()) {
                    deferrableUsed.set(true);
                }
                scheduledMails.add(command);
            }

            @Override
            public boolean shouldInterrupt() {
                return false;
            }

            @Override
            public boolean tryYield() throws FlinkRuntimeException {
                return false;
            }

            @Override
            public void yield() throws FlinkRuntimeException {
                // No-op for test
            }
        };
    }

    private InternalTimerServiceImpl<?, ?> createMockTimerService(boolean completes) {
        @SuppressWarnings("unchecked")
        final InternalTimerServiceImpl<Object, Object> mock = mock(InternalTimerServiceImpl.class);
        try {
            when(mock.tryAdvanceWatermark(anyLong(), any())).thenReturn(completes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mock;
    }

    private InternalTimerServiceImpl<?, ?> createMockTimerServiceWithAttempts(
            AtomicInteger attemptCount, int succeedOnAttempt) {
        @SuppressWarnings("unchecked")
        final InternalTimerServiceImpl<Object, Object> mock = mock(InternalTimerServiceImpl.class);
        try {
            when(mock.tryAdvanceWatermark(anyLong(), any()))
                    .thenAnswer(
                            invocation -> {
                                int attempt = attemptCount.incrementAndGet();
                                return attempt >= succeedOnAttempt;
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mock;
    }
}
