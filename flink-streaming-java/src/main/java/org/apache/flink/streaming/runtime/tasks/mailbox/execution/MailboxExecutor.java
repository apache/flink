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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Interface for an {@link Executor} build around a {@link Mailbox}-based execution model.
 */
public interface MailboxExecutor extends Executor {

	/**
	 * Executes the given command at some time in the future in the mailbox thread.
	 *
	 * @param command the runnable task to add to the mailbox for execution.
	 * @throws RejectedExecutionException if this task cannot be accepted for execution, e.g. because the mailbox is
	 *                                    quiesced or closed.
	 */
	@Override
	void execute(@Nonnull Runnable command) throws RejectedExecutionException;

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
}
