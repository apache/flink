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
import java.util.Optional;

/**
 * A task mailbox wraps the basic {@link Mailbox} interface with a lifecycle of open -> (quiesced) -> closed.
 * In the open state, the mailbox supports put and take operations.
 * In the quiesced state, the mailbox supports only take operations.
 *
 * <p>Additionally, letters have a priority that can be used to retrieve only relevant letters.
 */
public interface TaskMailbox {
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
	 * Returns a mailbox view bound to all mails.
	 *
	 * @return the mailbox
	 */
	Mailbox getMainMailbox();

	/**
	 * Returns a mailbox view bound to the given priority.
	 *
	 * <p>Enqueuing letters (e.g., {@link Mailbox#putMail(Runnable, Object description)} and
	 * {@link Mailbox#putFirst(Runnable, Object description)}) will mark these letters to belong to the bound operator.
	 *
	 * <p>Similarly, only letters from the operator or any downstream operator are retrieved by {@link Mailbox#tryTakeMail()}
	 * and {@link Mailbox#takeMail()}.
	 *
	 * <p>Note, that the lifecycle of the view is strictly coupled to the lifecycle of this task mailbox.
	 *
	 * @param priority the operator to which to bind
	 * @return the bound mailbox
	 */
	Mailbox getDownstreamMailbox(int priority);

	/**
	 * Returns <code>true</code> if the mailbox contains mail.
	 */
	boolean hasMail();

	/**
	 * Enqueues the given letter to the mailbox and blocks until there is capacity for a successful put.
	 *
	 * <p>An optional description can be added to ease debugging and error-reporting. Any object can be passed on which
	 * {@link Object#toString()} is lazily invoked. In most cases, it should be a {@link String} or
	 * {@link org.apache.flink.streaming.runtime.tasks.mailbox.LazyString}. If no explicit description is taken, the
	 * command itself is used and {@code toString()} will be invoked on it.
	 *
	 * @param letter the letter to enqueue.
	 * @param priority the priority of the letter.
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putMail(@Nonnull Runnable letter, int priority, Object description) throws MailboxStateException;

	/**
	 * Adds the given action to the head of the mailbox.
	 *
	 * <p>An optional description can be added to ease debugging and error-reporting. Any object can be passed on which
	 * {@link Object#toString()} is lazily invoked. In most cases, it should be a {@link String} or
	 * {@link org.apache.flink.streaming.runtime.tasks.mailbox.LazyString}. If no explicit description is taken, the
	 * command itself is used and {@code toString()} will be invoked on it.
	 *
	 * @param priorityLetter action to enqueue to the head of the mailbox.
	 * @param description the optional description for the command that is used for debugging and error-reporting.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putFirst(@Nonnull Runnable priorityLetter, Object description) throws MailboxStateException;

	/**
	 * Returns an optional with either the oldest letter with a minimum priority from the mailbox (head of queue) if the
	 * mailbox is not empty or an empty optional otherwise.
	 *
	 * @param priority the minimum priority of the letter.
	 * @return an optional with either the oldest letter from the mailbox (head of queue) if the mailbox is not empty or
	 * an empty optional otherwise.
	 * @throws MailboxStateException if mailbox is already closed.
	 */
	Optional<Runnable> tryTakeMail(int priority) throws MailboxStateException;

	/**
	 * This method returns the oldest letter with a minimum priority from the mailbox (head of queue) or blocks until a
	 * letter is available.
	 *
	 * @param priority the minimum priority of the letter.
	 * @return the oldest letter from the mailbox (head of queue).
	 * @throws InterruptedException on interruption.
	 * @throws MailboxStateException if mailbox is already closed.
	 */
	@Nonnull Runnable takeMail(int priority) throws InterruptedException, MailboxStateException;
}
