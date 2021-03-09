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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.concurrent.FutureTaskWithException;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * {@link java.util.concurrent.Executor} like interface for an build around a mailbox-based
 * execution model (see {@link TaskMailbox}). {@code MailboxExecutor} can also execute downstream
 * messages of a mailbox by yielding control from the task thread.
 *
 * <p>All submission functions can be called from any thread and will enqueue the action for further
 * processing in a FIFO fashion.
 *
 * <p>The yielding functions avoid the following situation: One operator cannot fully process an
 * input record and blocks the task thread until some resources are available. However, since the
 * introduction of the mailbox model blocking the task thread will not only block new inputs but
 * also all events from being processed. If the resources depend on downstream operators being able
 * to process such events (e.g., timers), then we may easily arrive at some livelocks.
 *
 * <p>The yielding functions will only process events from the operator itself and any downstream
 * operator. Events of upstream operators are only processed when the input has been fully processed
 * or if they yield themselves. This method avoid congestion and potential deadlocks, but will
 * process {@link Mail}s slightly out-of-order, effectively creating a view on the mailbox that
 * contains no message from upstream operators.
 *
 * <p><b>All yielding functions must be called in the mailbox thread</b> (see {@link
 * TaskMailbox#isMailboxThread()}) to not violate the single-threaded execution model. There are two
 * typical cases, both waiting until the resource is available. The main difference is if the
 * resource becomes available through a mailbox message itself or not.
 *
 * <p>If the resource becomes available through a mailbox mail, we can effectively block the task
 * thread. Implicitly, this requires the mail to be enqueued by a different thread.
 *
 * <pre>{@code
 * while (resource not available) {
 *     mailboxExecutor.yield();
 * }
 * }</pre>
 *
 * <p>If the resource becomes available through an external mechanism or the corresponding mail
 * needs to be enqueued in the task thread, we cannot block.
 *
 * <pre>{@code
 * while (resource not available) {
 *     if (!mailboxExecutor.tryYield()) {
 *         do stuff or sleep for a small amount of time
 *     }
 * }
 * }</pre>
 */
@PublicEvolving
public interface MailboxExecutor {
    /** A constant for empty args to save on object allocation. */
    Object[] EMPTY_ARGS = new Object[0];

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the runnable task to add to the mailbox for execution.
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default void execute(ThrowingRunnable<? extends Exception> command, String description) {
        execute(command, description, EMPTY_ARGS);
    }

    /**
     * Executes the given command at some time in the future in the mailbox thread.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the runnable task to add to the mailbox for execution.
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    void execute(
            ThrowingRunnable<? extends Exception> command,
            String descriptionFormat,
            Object... descriptionArgs);

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull Future<Void> submit(
            @Nonnull RunnableWithException command,
            String descriptionFormat,
            Object... descriptionArgs) {
        FutureTaskWithException<Void> future = new FutureTaskWithException<>(command);
        execute(future, descriptionFormat, descriptionArgs);
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull Future<Void> submit(
            @Nonnull RunnableWithException command, String description) {
        FutureTaskWithException<Void> future = new FutureTaskWithException<>(command);
        execute(future, description, EMPTY_ARGS);
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull <T> Future<T> submit(
            @Nonnull Callable<T> command, String descriptionFormat, Object... descriptionArgs) {
        FutureTaskWithException<T> future = new FutureTaskWithException<>(command);
        execute(future, descriptionFormat, descriptionArgs);
        return future;
    }

    /**
     * Submits the given command for execution in the future in the mailbox thread and returns a
     * Future representing that command. The Future's {@code get} method will return {@code null}
     * upon <em>successful</em> completion.
     *
     * <p>An optional description can (and should) be added to ease debugging and error-reporting.
     * The description may contain placeholder that refer to the provided description arguments
     * using {@link java.util.Formatter} syntax. The actual description is only formatted on demand.
     *
     * @param command the command to submit
     * @param description the optional description for the command that is used for debugging and
     *     error-reporting.
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g.
     *     because the mailbox is quiesced or closed.
     */
    default @Nonnull <T> Future<T> submit(@Nonnull Callable<T> command, String description) {
        FutureTaskWithException<T> future = new FutureTaskWithException<>(command);
        execute(future, description, EMPTY_ARGS);
        return future;
    }

    /**
     * This methods starts running the command at the head of the mailbox and is intended to be used
     * by the mailbox thread to yield from a currently ongoing action to another command. The method
     * blocks until another command to run is available in the mailbox and must only be called from
     * the mailbox thread. Must only be called from the mailbox thread to not violate the
     * single-threaded execution model.
     *
     * @throws InterruptedException on interruption.
     * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for
     *     yielding.
     * @throws FlinkRuntimeException if executed {@link RunnableWithException} thrown an exception.
     */
    void yield() throws InterruptedException, FlinkRuntimeException;

    /**
     * This methods attempts to run the command at the head of the mailbox. This is intended to be
     * used by the mailbox thread to yield from a currently ongoing action to another command. The
     * method returns true if a command was found and executed or false if the mailbox was empty.
     * Must only be called from the mailbox thread to not violate the single-threaded execution
     * model.
     *
     * @return true on successful yielding to another command, false if there was no command to
     *     yield to.
     * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for
     *     yielding.
     * @throws RuntimeException if executed {@link RunnableWithException} thrown an exception.
     */
    boolean tryYield() throws FlinkRuntimeException;
}
