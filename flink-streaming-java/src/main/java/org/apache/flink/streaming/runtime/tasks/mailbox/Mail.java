/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.Future;

/**
 * An executable bound to a specific operator in the chain, such that it can be picked for
 * downstream mailbox.
 */
@Internal
public class Mail {
    /** The action to execute. */
    private final ThrowingRunnable<? extends Exception> runnable;
    /**
     * The priority of the mail. The priority does not determine the order, but helps to hide
     * upstream mails from downstream processors to avoid live/deadlocks.
     */
    private final int priority;
    /** The description of the mail that is used for debugging and error-reporting. */
    private final String descriptionFormat;

    private final Object[] descriptionArgs;

    private final StreamTaskActionExecutor actionExecutor;

    public Mail(
            ThrowingRunnable<? extends Exception> runnable,
            int priority,
            String descriptionFormat,
            Object... descriptionArgs) {
        this(
                runnable,
                priority,
                StreamTaskActionExecutor.IMMEDIATE,
                descriptionFormat,
                descriptionArgs);
    }

    public Mail(
            ThrowingRunnable<? extends Exception> runnable,
            int priority,
            StreamTaskActionExecutor actionExecutor,
            String descriptionFormat,
            Object... descriptionArgs) {
        this.runnable = Preconditions.checkNotNull(runnable);
        this.priority = priority;
        this.descriptionFormat =
                descriptionFormat == null ? runnable.toString() : descriptionFormat;
        this.descriptionArgs = Preconditions.checkNotNull(descriptionArgs);
        this.actionExecutor = actionExecutor;
    }

    public int getPriority() {
        return priority;
    }

    public void tryCancel(boolean mayInterruptIfRunning) {
        if (runnable instanceof Future) {
            ((Future<?>) runnable).cancel(mayInterruptIfRunning);
        }
    }

    @Override
    public String toString() {
        return String.format(descriptionFormat, descriptionArgs);
    }

    public void run() throws Exception {
        actionExecutor.runThrowing(runnable);
    }
}
