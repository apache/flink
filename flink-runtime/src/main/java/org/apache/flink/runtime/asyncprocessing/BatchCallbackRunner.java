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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.apache.flink.core.state.StateFutureImpl.CallbackRunner} that put one mail in {@link
 * MailboxExecutor} but run multiple callbacks within one mail.
 */
public class BatchCallbackRunner {

    static final int DEFAULT_BATCH_SIZE = 3000;

    private final MailboxExecutor mailboxExecutor;

    private final int batchSize;

    /** The callbacks divided in batch. */
    private final ConcurrentLinkedDeque<ArrayList<ThrowingRunnable<? extends Exception>>>
            callbackQueue;

    /** The lock to protect the active buffer (batch). */
    private final Object activeBufferLock = new Object();

    /** The active buffer (batch) to gather incoming callbacks. */
    @GuardedBy("activeBufferLock")
    private ArrayList<ThrowingRunnable<? extends Exception>> activeBuffer;

    /** Counter of current callbacks. */
    private final AtomicInteger currentCallbacks = new AtomicInteger(0);

    /** Whether there is a mail in mailbox. */
    private volatile boolean hasMail = false;

    /** The logic to notify new mails to AEC. */
    private final Runnable newMailNotify;

    BatchCallbackRunner(MailboxExecutor mailboxExecutor, Runnable newMailNotify) {
        this.mailboxExecutor = mailboxExecutor;
        this.newMailNotify = newMailNotify;
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.callbackQueue = new ConcurrentLinkedDeque<>();
        this.activeBuffer = new ArrayList<>();
    }

    /**
     * Submit a callback to run.
     *
     * @param task the callback.
     */
    public void submit(ThrowingRunnable<? extends Exception> task) {
        synchronized (activeBufferLock) {
            activeBuffer.add(task);
            if (activeBuffer.size() >= batchSize) {
                callbackQueue.offerLast(activeBuffer);
                activeBuffer = new ArrayList<>(batchSize);
            }
        }
        if (currentCallbacks.getAndIncrement() == 0) {
            // Only when the single first callback is inserted, the mail should be inserted if not
            // exist. Otherwise, the #runBatch() from task threads will keep the mail exist.
            insertMail(false, true);
        }
    }

    /**
     * Insert a mail for a batch of mails.
     *
     * @param force force check if there should insert a callback regardless of the #hasMail flag.
     * @param notify should notify the AEC (typically on the task thread) if new mail inserted.
     */
    private void insertMail(boolean force, boolean notify) {
        // This method will be invoked via all ForSt I/O threads(from #submit) or task thread (from
        // #runBatch()). The #hasMail flag should be protected by synchronized.
        synchronized (this) {
            if (force || !hasMail) {
                if (currentCallbacks.get() > 0) {
                    hasMail = true;
                    mailboxExecutor.execute(
                            this::runBatch, "Batch running callback of state requests");
                    if (notify) {
                        notifyNewMail();
                    }
                } else {
                    hasMail = false;
                }
            }
        }
    }

    /**
     * Run at most a full batch of callbacks. If there has not been a full batch of callbacks, run
     * current callbacks in active buffer.
     */
    public void runBatch() throws Exception {
        ArrayList<ThrowingRunnable<? extends Exception>> batch = callbackQueue.poll();
        if (batch == null) {
            synchronized (activeBufferLock) {
                if (!activeBuffer.isEmpty()) {
                    batch = activeBuffer;
                    activeBuffer = new ArrayList<>(batchSize);
                }
            }
        }
        if (batch != null) {
            for (ThrowingRunnable<? extends Exception> task : batch) {
                task.run();
            }
            currentCallbacks.addAndGet(-batch.size());
        }
        // If we are on the task thread, there is no need to notify.
        insertMail(true, false);
    }

    private void notifyNewMail() {
        if (newMailNotify != null) {
            newMailNotify.run();
        }
    }

    public boolean isHasMail() {
        return hasMail;
    }
}
