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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import javax.annotation.Nonnull;

import java.util.List;

/**
 * A task mailbox wraps the basic {@link Mailbox} interface with a lifecycle of open -> (quiesced) -> closed.
 * In the open state, the mailbox supports put and take operations.
 * In the quiesced state, the mailbox supports only take operations.
 *
 * <p>Additionally, letters have a priority that can be used to retrieve only relevant letters.
 */
public interface TaskMailbox extends Mailbox {
	/**
	 * The minimal priority for letters. The priority is used when no operator is associated with the letter.
	 */
	int MIN_PRIORITY = -1;
	/**
	 * The maximal priority for letters. This priority indicates that the message should be performed before any letter
	 * associated with an operator.
	 */
	int MAX_PRIORITY = Integer.MAX_VALUE;

	/**
	 * This enum represents the states of the mailbox lifecycle.
	 */
	enum State {
		OPEN, QUIESCED, CLOSED
	}

	/**
	 * Quiesce the mailbox. In this state, the mailbox supports only take operations and all pending and future put
	 * operations will throw {@link MailboxStateException}.
	 */
	void quiesce();

	/**
	 * Close the mailbox. In this state, all pending and future put operations and all pending and future take
	 * operations will throw {@link MailboxStateException}. Returns all letters that were still enqueued.
	 *
	 * @return list with all letters that where enqueued in the mailbox at the time of closing.
	 */
	@Nonnull
	List<Runnable> close();

	/**
	 * Returns the current state of the mailbox as defined by the lifecycle enum {@link State}.
	 *
	 * @return the current state of the mailbox.
	 */
	@Nonnull
	State getState();

	/**
	 * Returns <code>true</code> if the mailbox contains mail.
	 */
	boolean hasMail();

	/**
	 * Runs the given code exclusively on this mailbox. No synchronized operations can be run concurrently to the
	 * given runnable (e.g., {@link #put(Mail)} or modifying lifecycle methods).
	 *
	 * <p>Use this methods when you want to atomically execute code that uses different methods (e.g., check for
	 * state and then put message if open).
	 *
	 * @param runnable the runnable to execute
	 */
	void runExclusively(Runnable runnable);
}
