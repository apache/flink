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
 * A mailbox is basically a queue for inter-thread message exchange in form of {@link Runnable} objects between
 * multiple producer threads and a single consumer. This has a lifecycle of closed -> open -> (quiesced) -> closed.
 */
public interface Mailbox extends MailboxReceiver, MailboxSender {

	/**
	 * This enum represents the states of the mailbox lifecycle.
	 */
	enum State {
		OPEN, QUIESCED, CLOSED
	}

	/**
	 * Open the mailbox. In this state, the mailbox supports put and take operations.
	 */
	void open();

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
	 * Adds the given action to the head of the mailbox.
	 *
	 * @param priorityLetter action to enqueue to the head of the mailbox.
	 * @throws MailboxStateException if the mailbox is quiesced or closed.
	 */
	void putFirst(@Nonnull Runnable priorityLetter) throws MailboxStateException;

	/**
	 * Returns the current state of the mailbox as defined by the lifecycle enum {@link State}.
	 *
	 * @return the current state of the mailbox.
	 */
	@Nonnull
	State getState();
}
