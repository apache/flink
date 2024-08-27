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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class to let operators emit watermarks incrementally from mailbox. Instead of emitting
 * all the watermarks at once in a single {@code processWatermark} call, if a mail in mailbox is
 * present, the process of firing timers is interrupted and a continuation to finish it off later is
 * scheduled via a mailbox mail.
 *
 * <p>Note that interrupting firing timers can change order of some invocations. It is possible that
 * between firing timers, some records might be processed.
 */
@Internal
public class MailboxWatermarkProcessor<OUT> {
    protected static final Logger LOG = LoggerFactory.getLogger(MailboxWatermarkProcessor.class);

    private final Output<StreamRecord<OUT>> output;
    private final MailboxExecutor mailboxExecutor;
    private final InternalTimeServiceManager<?> internalTimeServiceManager;
    /**
     * Flag to indicate whether a progress watermark is scheduled in the mailbox. This is used to
     * avoid duplicate scheduling in case we have multiple watermarks to process.
     */
    private boolean progressWatermarkScheduled = false;

    private Watermark maxInputWatermark = Watermark.UNINITIALIZED;

    public MailboxWatermarkProcessor(
            Output<StreamRecord<OUT>> output,
            MailboxExecutor mailboxExecutor,
            InternalTimeServiceManager<?> internalTimeServiceManager) {
        this.output = checkNotNull(output);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.internalTimeServiceManager = checkNotNull(internalTimeServiceManager);
    }

    public void emitWatermarkInsideMailbox(Watermark mark) throws Exception {
        maxInputWatermark =
                new Watermark(Math.max(maxInputWatermark.getTimestamp(), mark.getTimestamp()));
        emitWatermarkInsideMailbox();
    }

    private void emitWatermarkInsideMailbox() throws Exception {
        // Try to progress min watermark as far as we can.
        if (internalTimeServiceManager.tryAdvanceWatermark(
                maxInputWatermark, mailboxExecutor::shouldInterrupt)) {
            // In case output watermark has fully progressed emit it downstream.
            output.emitWatermark(maxInputWatermark);
        } else if (!progressWatermarkScheduled) {
            progressWatermarkScheduled = true;
            // We still have work to do, but we need to let other mails to be processed first.
            mailboxExecutor.execute(
                    MailboxExecutor.MailOptions.deferrable(),
                    () -> {
                        progressWatermarkScheduled = false;
                        emitWatermarkInsideMailbox();
                    },
                    "emitWatermarkInsideMailbox");
        } else {
            // We're not guaranteed that MailboxProcessor is going to process all mails before
            // processing additional input, so the advanceWatermark could be called before the
            // previous watermark is fully processed.
            LOG.debug("emitWatermarkInsideMailbox is already scheduled, skipping.");
        }
    }
}
