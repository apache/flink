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
 * A runner for {@link StateFutureFactory} to build {@link
 * org.apache.flink.core.state.InternalStateFuture} that put one mail in {@link MailboxExecutor}
 * whenever there are callbacks to run but run multiple callbacks within one mail.
 */
public class BatchCallbackRunner {

    private static final int DEFAULT_BATCH_SIZE = 100;

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

    BatchCallbackRunner(MailboxExecutor mailboxExecutor) {
        this.mailboxExecutor = mailboxExecutor;
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
        currentCallbacks.incrementAndGet();
        insertMail(false);
    }

    private void insertMail(boolean force) {
        if (force || !hasMail) {
            if (currentCallbacks.get() > 0) {
                hasMail = true;
                mailboxExecutor.execute(this::runBatch, "Batch running callback of state requests");
            } else {
                hasMail = false;
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
        insertMail(true);
    }
}
