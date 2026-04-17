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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.MailboxExecutor.MailOptions;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.MailboxWatermarkProcessor;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A specialized {@link MailboxWatermarkProcessor} for {@link AbstractProcessTableOperator} that
 * manages a list of {@link InternalTimerServiceImpl} instances directly instead of delegating to
 * {@link InternalTimeServiceManager}.
 *
 * <p>This processor advances watermarks across multiple timer services in a controlled manner,
 * allowing for interruptible timer firing via mailbox execution. When any mail is present in the
 * mailbox, the process of firing timers is interrupted and a continuation is scheduled to finish it
 * later.
 *
 * <p>When the watermark has been fully advanced across all timer services, the derived watermark is
 * made available via a consumer callback. This watermark represents the progress of the managed
 * timer services only, not a global watermark.
 *
 * <p>Note that interrupting firing timers can change the order of some invocations. It is possible
 * that between firing timers, some records might be processed.
 */
@Internal
public class MailboxPartialWatermarkProcessor {
    protected static final Logger LOG =
            LoggerFactory.getLogger(MailboxPartialWatermarkProcessor.class);

    private final String description;
    private final MailboxExecutor mailboxExecutor;
    private final List<InternalTimerServiceImpl<?, ?>> timerServices;
    private final WatermarkConsumer watermarkConsumer;

    /**
     * Flag to indicate whether a progress watermark is scheduled in the mailbox. This is used to
     * avoid duplicate scheduling in case we have multiple watermarks to process.
     */
    private boolean progressWatermarkScheduled = false;

    private Watermark maxInputWatermark = Watermark.UNINITIALIZED;

    public MailboxPartialWatermarkProcessor(
            String description,
            MailboxExecutor mailboxExecutor,
            List<InternalTimerServiceImpl<?, ?>> timerServices,
            WatermarkConsumer watermarkConsumer) {
        this.description = checkNotNull(description);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.timerServices = checkNotNull(timerServices);
        this.watermarkConsumer = checkNotNull(watermarkConsumer);
    }

    public void emitWatermarkInsideMailbox(Watermark mark) throws Exception {
        maxInputWatermark =
                new Watermark(Math.max(maxInputWatermark.getTimestamp(), mark.getTimestamp()));
        emitWatermarkInsideMailbox();
    }

    private void emitWatermarkInsideMailbox() throws Exception {
        // Try to progress min watermark as far as we can.
        if (tryAdvanceWatermark(maxInputWatermark, mailboxExecutor::shouldInterrupt)) {
            // In case output watermark has fully progressed, notify the consumer.
            watermarkConsumer.accept(maxInputWatermark);
        } else if (!progressWatermarkScheduled) {
            progressWatermarkScheduled = true;
            // We still have work to do, but we need to let other mails to be processed first.
            mailboxExecutor.execute(
                    MailOptions.deferrable(),
                    () -> {
                        progressWatermarkScheduled = false;
                        emitWatermarkInsideMailbox();
                    },
                    description);
        } else {
            // We're not guaranteed that MailboxProcessor is going to process all mails before
            // processing additional input, so the advanceWatermark could be called before the
            // previous watermark is fully processed.
            LOG.debug("{} is already scheduled, skipping.", description);
        }
    }

    /**
     * Attempts to advance the watermark across all timer services. If the {@code
     * shouldStopAdvancingFn} returns true during processing, the advancement is interrupted and can
     * be resumed later.
     *
     * @param watermark the target watermark to advance to
     * @param shouldStopAdvancingFn function that indicates whether to interrupt the advancement
     * @return true if the watermark has been fully processed across all timer services, false
     *     otherwise
     */
    private boolean tryAdvanceWatermark(
            Watermark watermark, ShouldStopAdvancingFn shouldStopAdvancingFn) throws Exception {
        final long targetWatermark = watermark.getTimestamp();

        // Advance watermark across all timer services
        for (InternalTimerServiceImpl<?, ?> timerService : timerServices) {
            if (!timerService.tryAdvanceWatermark(targetWatermark, shouldStopAdvancingFn::test)) {
                // Interrupted while processing this timer service
                return false;
            }
        }

        // All timer services have been fully processed
        return true;
    }

    /** Signals whether the watermark should continue advancing. */
    @FunctionalInterface
    public interface ShouldStopAdvancingFn {

        /**
         * @return {@code true} if firing timers should be interrupted.
         */
        boolean test();
    }

    /** Consumer when a watermark has been fully processed by the mailbox. */
    @FunctionalInterface
    public interface WatermarkConsumer {

        void accept(Watermark watermark) throws Exception;
    }
}
