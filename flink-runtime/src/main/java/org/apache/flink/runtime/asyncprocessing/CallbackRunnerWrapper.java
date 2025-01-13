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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.apache.flink.core.state.StateFutureImpl.CallbackRunner} that gives info of {@link
 * #isHasMail()} to the AEC and notifies new mail if needed.
 */
public class CallbackRunnerWrapper {

    private final MailboxExecutor mailboxExecutor;

    /** Counter of current callbacks. */
    private final AtomicInteger currentCallbacks = new AtomicInteger(0);

    /** The logic to notify new mails to AEC. */
    private final Runnable newMailNotify;

    CallbackRunnerWrapper(MailboxExecutor mailboxExecutor, Runnable newMailNotify) {
        this.mailboxExecutor = mailboxExecutor;
        this.newMailNotify = newMailNotify;
    }

    /**
     * Submit a callback to run.
     *
     * @param task the callback.
     */
    public void submit(ThrowingRunnable<? extends Exception> task) {
        mailboxExecutor.execute(
                () -> {
                    // -1 before the task run, since the task may query #isHasMail, and we don't
                    // want to return to true before the task is finished if there is no else mail.
                    currentCallbacks.decrementAndGet();
                    task.run();
                },
                "Callback of state request");
        if (currentCallbacks.getAndIncrement() == 0) {
            notifyNewMail();
        }
    }

    private void notifyNewMail() {
        if (newMailNotify != null) {
            newMailNotify.run();
        }
    }

    public boolean isHasMail() {
        return currentCallbacks.get() > 0;
    }
}
