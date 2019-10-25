/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox.execution;

import org.apache.flink.streaming.runtime.tasks.mailbox.Mailbox;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

/**
 * Interface for an {@link Executor} build around a {@link Mailbox}-based execution model.
 */
public interface MailboxExecutor {
	/**
	 * A constant for empty args to save on object allocation.
	 */
	Object[] EMPTY_ARGS = new Object[0];

	/**
	 * Executes the given command at some time in the future in the mailbox thread.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the runnable task to add to the mailbox for execution.
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * 		quiesced or closed.
	 */
	default void execute(@Nonnull Runnable command, String description) {
		execute(command, description, EMPTY_ARGS);
	}

	/**
	 * Executes the given command at some time in the future in the mailbox thread.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the runnable task to add to the mailbox for execution.
	 * @param descriptionFormat the optional description for the command that is used for debugging and error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * 		quiesced or closed.
	 */
	void execute(@Nonnull Runnable command, String descriptionFormat, Object... descriptionArgs);

	/**
	 * Executes the given command at the next possible time in the mailbox thread. Repeated calls will result in the
	 * last executed command being executed first.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the runnable task to add to the mailbox for execution.
	 * @param descriptionFormat the optional description for the command that is used for debugging and error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 *                                    quiesced or closed.
	 */
	void executeFirst(@Nonnull Runnable command, String descriptionFormat, Object... descriptionArgs);

	/**
	 * Submits the given command for execution in the future in the mailbox thread and returns a Future representing
	 * that command. The Future's {@code get} method will return {@code null} upon <em>successful</em> completion.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the command to submit
	 * @param descriptionFormat the optional description for the command that is used for debugging and error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 * @return a Future representing pending completion of the task
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * quiesced or closed.
	 */
	default @Nonnull Future<Void> submit(@Nonnull Runnable command, String descriptionFormat, Object... descriptionArgs) {
		FutureTask<Void> future = new FutureTask<>(Executors.callable(command, null));
		execute(future, descriptionFormat, descriptionArgs);
		return future;
	}

	/**
	 * Submits the given command for execution in the future in the mailbox thread and returns a Future representing
	 * that command. The Future's {@code get} method will return {@code null} upon <em>successful</em> completion.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the command to submit
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @return a Future representing pending completion of the task
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * quiesced or closed.
	 */
	default @Nonnull Future<Void> submit(@Nonnull Runnable command, String description) {
		FutureTask<Void> future = new FutureTask<>(Executors.callable(command, null));
		execute(future, description, EMPTY_ARGS);
		return future;
	}

	/**
	 * Submits the given command for execution in the future in the mailbox thread and returns a Future representing
	 * that command. The Future's {@code get} method will return {@code null} upon <em>successful</em> completion.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the command to submit
	 * @param descriptionFormat the optional description for the command that is used for debugging and error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 * @return a Future representing pending completion of the task
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * quiesced or closed.
	 */
	default @Nonnull <T> Future<T> submit(@Nonnull Callable<T> command, String descriptionFormat, Object... descriptionArgs) {
		FutureTask<T> future = new FutureTask<>(command);
		execute(future, descriptionFormat, descriptionArgs);
		return future;
	}

	/**
	 * Submits the given command for execution in the future in the mailbox thread and returns a Future representing
	 * that command. The Future's {@code get} method will return {@code null} upon <em>successful</em> completion.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param command the command to submit
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @return a Future representing pending completion of the task
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 * quiesced or closed.
	 */
	default @Nonnull <T> Future<T> submit(@Nonnull Callable<T> command, String description) {
		FutureTask<T> future = new FutureTask<>(command);
		execute(future, description, EMPTY_ARGS);
		return future;
	}

	/**
	 * This methods starts running the command at the head of the mailbox and is intended to be used by the mailbox
	 * thread to yield from a currently ongoing action to another command. The method blocks until another command to
	 * run is available in the mailbox and must only be called from the mailbox thread. Must only be called from the
	 * mailbox thread to not violate the single-threaded execution model.
	 *
	 * @throws InterruptedException on interruption.
	 * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for yielding.
	 */
	void yield() throws InterruptedException, IllegalStateException;

	/**
	 * This methods attempts to run the command at the head of the mailbox. This is intended to be used by the mailbox
	 * thread to yield from a currently ongoing action to another command. The method returns true if a command was
	 * found and executed or false if the mailbox was empty. Must only be called from the
	 * mailbox thread to not violate the single-threaded execution model.
	 *
	 * @return true on successful yielding to another command, false if there was no command to yield to.
	 * @throws IllegalStateException if the mailbox is closed and can no longer supply runnables for yielding.
	 */
	boolean tryYield() throws IllegalStateException;

	/**
	 * Check if the current thread is the mailbox thread.
	 *
	 * @return only true if called from the mailbox thread.
	 */
	boolean isMailboxThread();

	/**
	 * Provides an {@link Executor} view on this {@code MailboxExecutor}, where submitted tasks will receive the
	 * given description. The {@link Executor} can be used with {@link java.util.concurrent.CompletableFuture}.
	 *
	 * <p>An optional description can (and should) be added to ease debugging and error-reporting. The description
	 * may contain placeholder that refer to the provided description arguments using {@link java.util.Formatter}
	 * syntax. The actual description is only formatted on demand.
	 *
	 * @param descriptionFormat the optional description for all commands that is used for debugging and
	 * error-reporting.
	 * @param descriptionArgs the parameters used to format the final description string.
	 * @return an {@code Executor} view on this {@code MailboxExecutor}
	 */
	default Executor asExecutor(String descriptionFormat, Object... descriptionArgs) {
		return command -> execute(command, descriptionFormat, descriptionArgs);
	}
}
